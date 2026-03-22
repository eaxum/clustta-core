package sync_service

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/DataDog/zstd"
	"github.com/jmoiron/sqlx"
	"google.golang.org/protobuf/proto"

	"github.com/eaxum/clustta-core/constants"
	error_service "github.com/eaxum/clustta-core/errors"
	"github.com/eaxum/clustta-core/repository"
	"github.com/eaxum/clustta-core/repository/models"
	"github.com/eaxum/clustta-core/repository/repositorypb"
	"github.com/eaxum/clustta-core/utils"
)

// ConflictInfo describes a naming conflict detected during push.
type ConflictInfo struct {
	Type       string `json:"type"`
	LocalId    string `json:"local_id"`
	ExistingId string `json:"existing_id"`
	Name       string `json:"name"`
	ParentId   string `json:"parent_id"`
	Extension  string `json:"extension"`
}

// WriteResult is the server response for a data push.
type WriteResult struct {
	Success   bool           `json:"success"`
	Conflicts []ConflictInfo `json:"conflicts,omitempty"`
}

// SyncConflictError is returned when a push detects naming conflicts.
type SyncConflictError struct {
	Conflicts []ConflictInfo
}

func (e *SyncConflictError) Error() string {
	return fmt.Sprintf("sync conflict: %d items conflict", len(e.Conflicts))
}

// ProjectData holds all domain data for a project sync operation.
type ProjectData struct {
	ProjectPreview         string                        `json:"project_preview"`
	Assets                 []models.Asset                `json:"assets"`
	AssetTypes             []models.AssetType            `json:"asset_types"`
	AssetCheckpoints       []models.Checkpoint           `json:"assets_checkpoints"`
	AssetDependencies      []models.AssetDependency      `json:"asset_dependencies"`
	CollectionDependencies []models.CollectionDependency `json:"collection_dependencies"`

	Statuses        []models.Status         `json:"statuses"`
	DependencyTypes []models.DependencyType `json:"dependency_types"`

	Users []models.User `json:"users"`
	Roles []models.Role `json:"roles"`

	CollectionTypes     []models.CollectionType     `json:"collection_types"`
	Collections         []models.Collection         `json:"collections"`
	CollectionAssignees []models.CollectionAssignee `json:"collection_assignees"`

	Templates []models.Template `json:"templates"`
	Tags      []models.Tag      `json:"tags"`
	AssetTags []models.AssetTag `json:"assets_tags"`

	Workflows           []models.Workflow           `json:"workflows"`
	WorkflowLinks       []models.WorkflowLink       `json:"workflow_links"`
	WorkflowCollections []models.WorkflowCollection `json:"workflow_collections"`
	WorkflowAssets      []models.WorkflowAsset      `json:"workflow_assets"`

	Tombs []repository.Tomb `json:"tomb"`

	IntegrationProjects           []models.IntegrationProject           `json:"integration_projects"`
	IntegrationCollectionMappings []models.IntegrationCollectionMapping `json:"integration_collection_mappings"`
	IntegrationAssetMappings      []models.IntegrationAssetMapping      `json:"integration_asset_mappings"`
}

// IsEmpty returns true if the ProjectData contains no data to sync.
func (d *ProjectData) IsEmpty() bool {
	return len(d.Assets) == 0 &&
		len(d.AssetTypes) == 0 &&
		len(d.AssetCheckpoints) == 0 &&
		len(d.AssetDependencies) == 0 &&
		len(d.CollectionDependencies) == 0 &&
		len(d.CollectionTypes) == 0 &&
		len(d.Collections) == 0 &&
		len(d.CollectionAssignees) == 0 &&
		len(d.Templates) == 0 &&
		len(d.Tags) == 0 &&
		len(d.AssetTags) == 0 &&
		len(d.Statuses) == 0 &&
		len(d.DependencyTypes) == 0 &&
		len(d.Users) == 0 &&
		len(d.Roles) == 0 &&
		len(d.Workflows) == 0 &&
		len(d.WorkflowLinks) == 0 &&
		len(d.WorkflowCollections) == 0 &&
		len(d.WorkflowAssets) == 0 &&
		len(d.Tombs) == 0 &&
		len(d.IntegrationProjects) == 0 &&
		len(d.IntegrationCollectionMappings) == 0 &&
		len(d.IntegrationAssetMappings) == 0 &&
		d.ProjectPreview == ""
}

// WriteProjectData merges incoming data into the local project database.
// When strict is true, all referenced chunks must already exist locally.
func WriteProjectData(tx *sqlx.Tx, data ProjectData, strict bool, chunkProvider repository.ChunkProvider) error {

	sortedCollections, err := repository.TopologicalSort(data.Collections)
	if err != nil {
		return err
	}
	data.Collections = sortedCollections

	tombItems := make(map[string]bool)
	tombedItems, err := repository.GetTombedItems(tx)
	if err != nil {
		return err
	}
	for _, tombItem := range tombedItems {
		tombItems[tombItem] = true
	}

	chunks := []string{}
	for _, cp := range data.AssetCheckpoints {
		for _, h := range strings.Split(cp.Chunks, ",") {
			if !utils.Contains(chunks, h) {
				chunks = append(chunks, h)
			}
		}
	}
	for _, t := range data.Templates {
		for _, h := range strings.Split(t.Chunks, ",") {
			if !utils.Contains(chunks, h) {
				chunks = append(chunks, h)
			}
		}
	}
	if strict {
		missingChunks, err := chunkProvider.GetNonExistingChunks(tx, chunks)
		if err != nil {
			return err
		}
		if len(missingChunks) != 0 {
			return errors.New("data have missing chunks")
		}
	}

	previewIds := collectPreviewIds(data)
	missingPreviews, err := repository.GetNonExistingPreviews(tx, previewIds)
	if err != nil {
		return err
	}
	if len(missingPreviews) != 0 {
		return errors.New("data have missing previews")
	}

	if data.ProjectPreview != "" {
		_, err = tx.Exec(`
		INSERT INTO config (name, value, mtime, synced)
		VALUES ('project_preview', $1, $2, 1)
		ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value, mtime = EXCLUDED.mtime, synced = 1
	`, data.ProjectPreview, utils.GetEpochTime())
	}

	noopAuth := &repository.NoopAuthProvider{}

	for _, role := range data.Roles {
		if tombItems[role.Id] {
			continue
		}
		roleAttributes := roleAttributesFromRole(role)
		localRole, err := repository.GetRole(tx, role.Id)
		if err != nil {
			if errors.Is(err, error_service.ErrRoleNotFound) {
				_, err := repository.CreateRole(tx, role.Id, role.Name, roleAttributes)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		} else {
			if localRole.MTime < role.MTime {
				_, err = repository.UpdateRole(tx, role.Id, role.Name, roleAttributes)
				if err != nil {
					return err
				}
			}
		}
	}

	for _, user := range data.Users {
		localUser, err := repository.GetUser(tx, user.Id)
		if err != nil {
			if errors.Is(err, error_service.ErrUserNotFound) {
				_, err := repository.AddKnownUser(
					tx, user.Id, user.Email, user.Username,
					user.FirstName, user.LastName, user.RoleId, user.Photo, false, noopAuth)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		} else {
			if localUser.MTime < user.MTime {
				if localUser.RoleId != user.RoleId {
					err = repository.ChangeUserRole(tx, user.Id, user.RoleId)
					if err != nil {
						return err
					}
				}
			}
		}
	}

	for _, collectionType := range data.CollectionTypes {
		if tombItems[collectionType.Id] {
			continue
		}
		localCollectionType, err := repository.GetCollectionType(tx, collectionType.Id)
		if err != nil {
			if errors.Is(err, error_service.ErrCollectionTypeNotFound) {
				_, err = repository.CreateCollectionType(tx, collectionType.Id, collectionType.Name, collectionType.Icon)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		} else {
			if localCollectionType.MTime < collectionType.MTime {
				_, err = repository.UpdateCollectionType(tx, collectionType.Id, collectionType.Name, collectionType.Icon)
				if err != nil {
					return err
				}
			}
		}
	}

	for _, assetType := range data.AssetTypes {
		if tombItems[assetType.Id] {
			continue
		}
		localAssetType, err := repository.GetAssetType(tx, assetType.Id)
		if err != nil {
			if errors.Is(err, error_service.ErrAssetTypeNotFound) {
				_, err = repository.CreateAssetType(tx, assetType.Id, assetType.Name, assetType.Icon)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		} else {
			if localAssetType.MTime < assetType.MTime {
				_, err = repository.UpdateAssetType(tx, assetType.Id, assetType.Name, assetType.Icon)
				if err != nil {
					return err
				}
			}
		}
	}

	for _, dependencyType := range data.DependencyTypes {
		if tombItems[dependencyType.Id] {
			continue
		}
		_, err = repository.GetDependencyType(tx, dependencyType.Id)
		if err != nil {
			if errors.Is(err, error_service.ErrDependencyTypeNotFound) {
				_, err = repository.CreateDependencyType(tx, dependencyType.Id, dependencyType.Name)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
	}

	for _, status := range data.Statuses {
		if tombItems[status.Id] {
			continue
		}
		_, err = repository.GetStatus(tx, status.Id)
		if err != nil {
			if errors.Is(err, error_service.ErrStatusNotFound) {
				_, err = repository.CreateStatus(tx, status.Id, status.Name, status.ShortName, status.Color)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
	}

	for _, tag := range data.Tags {
		if tombItems[tag.Id] {
			continue
		}
		_, err = repository.GetTag(tx, tag.Id)
		if err != nil {
			if errors.Is(err, error_service.ErrTagNotFound) {
				_, err = repository.CreateTag(tx, tag.Id, tag.Name)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
	}

	localCollections, err := repository.GetSimpleCollections(tx)
	if err != nil {
		return err
	}
	localCollectionsIndex := make(map[string]int)
	for i, t := range localCollections {
		localCollectionsIndex[t.Id] = i
	}

	for _, collection := range data.Collections {
		if tombItems[collection.Id] {
			continue
		}
		i, exists := localCollectionsIndex[collection.Id]
		if !exists {
			err = repository.AddCollection(
				tx, collection.Id, collection.Name, collection.Description, collection.CollectionTypeId, collection.ParentId, collection.PreviewId, collection.IsLibrary)
			if err != nil {
				if err.Error() == "parent collection not found" {
					continue
				}
				return err
			}
			continue
		}

		localCollection := localCollections[i]
		if localCollection.MTime < collection.MTime {
			parentId := collection.ParentId
			previewId := collection.PreviewId
			isLibrary := collection.IsLibrary

			collection, err = repository.RenameCollection(tx, collection.Id, collection.Name)
			if err != nil {
				return err
			}

			collection.ParentId = parentId
			collection.PreviewId = previewId
			collection.IsLibrary = isLibrary

			if localCollection.ParentId != collection.ParentId {
				err = repository.ChangeParent(tx, collection.Id, collection.ParentId)
				if err != nil {
					return err
				}
			}

			if localCollection.PreviewId != collection.PreviewId {
				err = repository.SetCollectionPreview(tx, collection.Id, "collection", collection.PreviewId)
				if err != nil {
					return err
				}
			}
			if localCollection.IsLibrary != collection.IsLibrary {
				err = repository.ChangeIsLibrary(tx, collection.Id, collection.IsLibrary)
				if err != nil {
					return err
				}
			}
		}
	}

	for _, collectionAssignee := range data.CollectionAssignees {
		if tombItems[collectionAssignee.Id] {
			continue
		}
		_, err = repository.GetAssignee(tx, collectionAssignee.Id)
		if err != nil {
			if errors.Is(err, error_service.ErrCollectionAssigneeNotFound) {
				err = repository.AddAssignee(tx, collectionAssignee.Id, collectionAssignee.CollectionId, collectionAssignee.AssigneeId)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
	}

	localAssets, err := repository.GetSimpleAssets(tx)
	if err != nil {
		return err
	}
	localAssetsIndex := make(map[string]int)
	for i, t := range localAssets {
		localAssetsIndex[t.Id] = i
	}

	createAssetStmt, err := tx.Prepare(`
		INSERT INTO asset 
		(id, assignee_id, mtime, created_at, name, description, extension, asset_type_id, collection_id, is_resource, status_id, pointer, is_link, preview_id) 
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);
	`)
	if err != nil {
		return err
	}

	for _, asset := range data.Assets {
		if tombItems[asset.Id] {
			continue
		}

		i, exists := localAssetsIndex[asset.Id]
		if !exists {
			_, err := createAssetStmt.Exec(asset.Id, asset.AssigneeId, asset.MTime, asset.CreatedAt, asset.Name, asset.Description, asset.Extension, asset.AssetTypeId, asset.CollectionId, asset.IsResource, asset.StatusId, asset.Pointer, asset.IsLink, asset.PreviewId)
			if err != nil {
				return err
			}
			continue
		}

		localAsset := localAssets[i]
		if localAsset.MTime < asset.MTime {
			err := repository.UpdateSyncAsset(tx, asset.Id, asset.Name, asset.CollectionId, asset.AssetTypeId, asset.AssigneeId, asset.AssignerId, asset.StatusId, asset.PreviewId, asset.IsResource, asset.IsLink, asset.Pointer, []string{})
			if err != nil {
				return err
			}
		}
	}

	localCheckpoints, err := repository.GetSimpleCheckpoints(tx)
	if err != nil {
		return err
	}
	localCheckpointsIndex := make(map[string]int)
	for i, c := range localCheckpoints {
		localCheckpointsIndex[c.Id] = i
	}

	createCheckpointStmt, err := tx.Prepare(`
		INSERT INTO asset_checkpoint 
		(id, mtime, created_at, asset_id, xxhash_checksum, time_modified, file_size, comment, chunks, author_id, preview_id, group_id) 
		VALUES (?, ?,?,?,?,?,?,?,?,?,?,?);
	`)
	if err != nil {
		return err
	}

	for _, cp := range data.AssetCheckpoints {
		if tombItems[cp.Id] {
			continue
		}

		_, exists := localCheckpointsIndex[cp.Id]
		if !exists {
			epochTime, err := utils.RFC3339ToEpoch(cp.CreatedAt)
			if err != nil {
				return err
			}
			_, err = createCheckpointStmt.Exec(cp.Id, cp.MTime, epochTime, cp.AssetId, cp.XXHashChecksum, cp.TimeModified, cp.FileSize, cp.Comment, cp.Chunks, cp.AuthorUID, cp.PreviewId, cp.GroupId)
			if err != nil {
				return err
			}
		}
	}

	for _, dep := range data.AssetDependencies {
		if tombItems[dep.Id] {
			continue
		}
		_, err = repository.GetDependency(tx, dep.Id)
		if err != nil {
			if errors.Is(err, error_service.ErrAssetDependencyNotFound) {
				_, err = repository.AddDependency(tx, dep.Id, dep.AssetId, dep.DependencyId, dep.DependencyTypeId)
				if err != nil {
					if err.Error() == "UNIQUE constraint failed: asset_dependency.asset_id, asset_dependency.dependency_id" {
						continue
					}
					return err
				}
			} else {
				return err
			}
		}
	}

	for _, dep := range data.CollectionDependencies {
		if tombItems[dep.Id] {
			continue
		}
		_, err = repository.GetCollectionDependency(tx, dep.Id)
		if err != nil {
			if errors.Is(err, error_service.ErrCollectionDependencyNotFound) {
				_, err = repository.AddCollectionDependency(tx, dep.Id, dep.AssetId, dep.DependencyId, dep.DependencyTypeId)
				if err != nil {
					if err.Error() == "UNIQUE constraint failed: collection_dependency.asset_id, collection_dependency.dependency_id" {
						continue
					}
					return err
				}
			} else {
				return err
			}
		}
	}

	for _, template := range data.Templates {
		if tombItems[template.Id] {
			continue
		}
		_, err = repository.GetTemplate(tx, template.Id)
		if err != nil {
			if errors.Is(err, error_service.ErrTemplateNotFound) {
				_, err = repository.AddTemplate(tx, template.Id, template.Name, template.Extension, template.Chunks, template.XxhashChecksum, template.FileSize)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
	}

	for _, workflow := range data.Workflows {
		if tombItems[workflow.Id] {
			continue
		}
		localWorkflow, err := repository.GetWorkflow(tx, workflow.Id)
		if err != nil {
			if errors.Is(err, error_service.ErrWorkflowNotFound) {
				_, err = repository.CreateWorkflow(tx, workflow.Id, workflow.Name, []models.WorkflowAsset{}, []models.WorkflowCollection{}, []models.WorkflowLink{})
				if err != nil {
					return err
				}
			} else {
				return err
			}
		} else {
			if localWorkflow.MTime < workflow.MTime {
				err = repository.RenameWorkflow(tx, workflow.Id, workflow.Name)
				if err != nil {
					return err
				}
			}
		}
	}

	for _, wl := range data.WorkflowLinks {
		if tombItems[wl.Id] {
			continue
		}
		localWL, err := repository.GetWorkflowLink(tx, wl.Id)
		if err != nil {
			if errors.Is(err, error_service.ErrWorkflowLinkNotFound) {
				err = repository.AddLinkWorkflow(tx, wl.Id, wl.Name, wl.CollectionTypeId, wl.WorkflowId, wl.LinkedWorkflowId)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		} else {
			if localWL.MTime < wl.MTime {
				err = repository.RenameLinkedWorkflow(tx, wl.Id, wl.Name)
				if err != nil {
					return err
				}
			}
		}
	}

	for _, wc := range data.WorkflowCollections {
		if tombItems[wc.Id] {
			continue
		}
		localWC, err := repository.GetWorkflowCollection(tx, wc.Id)
		if err != nil {
			if errors.Is(err, error_service.ErrWorkflowCollectionNotFound) {
				_, err = repository.CreateWorkflowCollection(tx, wc.Id, wc.Name, wc.WorkflowId, wc.CollectionTypeId)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		} else {
			if localWC.MTime < wc.MTime {
				_, err = repository.UpdateWorkflowCollection(tx, wc.Id, wc.Name, wc.CollectionTypeId)
				if err != nil {
					return err
				}
			}
		}
	}

	for _, wa := range data.WorkflowAssets {
		if tombItems[wa.Id] {
			continue
		}
		localWA, err := repository.GetWorkflowAsset(tx, wa.Id)
		if err != nil {
			if errors.Is(err, error_service.ErrWorkflowAssetNotFound) {
				_, err = repository.CreateWorkflowAsset(tx, wa.Id, wa.Name, wa.WorkflowId, wa.AssetTypeId, wa.IsResource, wa.TemplateId, wa.Pointer, wa.IsLink)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		} else {
			if localWA.MTime < wa.MTime {
				_, err = repository.UpdateWorkflowAsset(tx, wa.Id, wa.Name, wa.AssetTypeId, wa.IsResource, wa.TemplateId, wa.Pointer, wa.IsLink)
				if err != nil {
					return err
				}
			}
		}
	}

	for _, assetTag := range data.AssetTags {
		if tombItems[assetTag.Id] {
			continue
		}
		_, err = repository.GetAssetTag(tx, assetTag.Id)
		if err != nil {
			if errors.Is(err, error_service.ErrAssetTagNotFound) {
				err = repository.AddTagToAssetById(tx, assetTag.Id, assetTag.AssetId, assetTag.TagId)
				if err != nil {
					return err
				}
			} else {
				return err
			}
		}
	}

	// Integration project
	for _, ip := range data.IntegrationProjects {
		if tombItems[ip.Id] {
			continue
		}
		localIP, err := repository.GetIntegrationProject(tx)
		if err != nil {
			_, err = repository.CreateIntegrationProject(
				tx, ip.Id, ip.IntegrationId, ip.ExternalProjectId,
				ip.ExternalProjectName, ip.ApiUrl, ip.SyncOptions,
				ip.LinkedByUserId, ip.LinkedAt)
			if err != nil {
				return err
			}
		} else {
			if localIP.MTime < ip.MTime {
				err = repository.UpdateIntegrationProject(tx, localIP.Id, map[string]interface{}{
					"integration_id":        ip.IntegrationId,
					"external_project_id":   ip.ExternalProjectId,
					"external_project_name": ip.ExternalProjectName,
					"api_url":               ip.ApiUrl,
					"sync_options":          ip.SyncOptions,
					"enabled":               ip.Enabled,
				})
				if err != nil {
					return err
				}
			}
		}
	}

	// Integration collection mappings
	for _, mapping := range data.IntegrationCollectionMappings {
		if tombItems[mapping.Id] {
			continue
		}
		localMapping, err := repository.GetCollectionMapping(tx, mapping.Id)
		if err != nil {
			_, err = repository.CreateCollectionMapping(
				tx, mapping.Id, mapping.IntegrationId, mapping.ExternalId, mapping.ExternalType,
				mapping.ExternalName, mapping.ExternalParentId, mapping.ExternalPath,
				mapping.ExternalMetadata, mapping.CollectionId, mapping.SyncedAt)
			if err != nil {
				return err
			}
		} else {
			if localMapping.MTime < mapping.MTime {
				err = repository.UpdateCollectionMapping(tx, mapping.Id, map[string]interface{}{
					"external_type":      mapping.ExternalType,
					"external_name":      mapping.ExternalName,
					"external_parent_id": mapping.ExternalParentId,
					"external_path":      mapping.ExternalPath,
					"external_metadata":  mapping.ExternalMetadata,
					"collection_id":      mapping.CollectionId,
					"synced_at":          mapping.SyncedAt,
				})
				if err != nil {
					return err
				}
			}
		}
	}

	// Integration asset mappings
	for _, mapping := range data.IntegrationAssetMappings {
		if tombItems[mapping.Id] {
			continue
		}
		localMapping, err := repository.GetAssetMapping(tx, mapping.Id)
		if err != nil {
			_, err = repository.CreateAssetMapping(
				tx, mapping.Id, mapping.IntegrationId, mapping.ExternalId, mapping.ExternalName,
				mapping.ExternalParentId, mapping.ExternalType, mapping.ExternalStatus,
				mapping.ExternalAssignees, mapping.ExternalMetadata, mapping.AssetId, mapping.SyncedAt)
			if err != nil {
				return err
			}
		} else {
			if localMapping.MTime < mapping.MTime {
				err = repository.UpdateAssetMapping(tx, mapping.Id, map[string]interface{}{
					"external_name":             mapping.ExternalName,
					"external_parent_id":        mapping.ExternalParentId,
					"external_type":             mapping.ExternalType,
					"external_status":           mapping.ExternalStatus,
					"external_assignees":        mapping.ExternalAssignees,
					"external_metadata":         mapping.ExternalMetadata,
					"asset_id":                  mapping.AssetId,
					"last_pushed_checkpoint_id": mapping.LastPushedCheckpointId,
					"synced_at":                 mapping.SyncedAt,
				})
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// OverWriteProjectData replaces all project data (used after clearing tables).
func OverWriteProjectData(tx *sqlx.Tx, data ProjectData) error {
	sortedCollections, err := repository.TopologicalSort(data.Collections)
	if err != nil {
		return err
	}
	data.Collections = sortedCollections

	previewIds := collectPreviewIds(data)
	missingPreviews, err := repository.GetNonExistingPreviews(tx, previewIds)
	if err != nil {
		return err
	}
	if len(missingPreviews) != 0 {
		return errors.New("data have missing previews")
	}

	noopAuth := &repository.NoopAuthProvider{}

	if data.ProjectPreview != "" {
		_, err = tx.Exec(`
		INSERT INTO config (name, value, mtime, synced)
		VALUES ('project_preview', $1, $2, 1)
		ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value, mtime = EXCLUDED.mtime, synced = 1
	`, data.ProjectPreview, utils.GetEpochTime())
		if err != nil {
			return err
		}
	}

	for _, role := range data.Roles {
		roleAttributes := roleAttributesFromRole(role)
		_, err := repository.CreateRole(tx, role.Id, role.Name, roleAttributes)
		if err != nil {
			return err
		}
	}

	for _, user := range data.Users {
		_, err := repository.AddKnownUser(
			tx, user.Id, user.Email, user.Username,
			user.FirstName, user.LastName, user.RoleId, user.Photo, false, noopAuth)
		if err != nil {
			return err
		}
	}

	for _, ct := range data.CollectionTypes {
		_, err = repository.CreateCollectionType(tx, ct.Id, ct.Name, ct.Icon)
		if err != nil {
			return err
		}
	}

	for _, at := range data.AssetTypes {
		_, err = repository.CreateAssetType(tx, at.Id, at.Name, at.Icon)
		if err != nil {
			return err
		}
	}

	for _, dt := range data.DependencyTypes {
		_, err = repository.CreateDependencyType(tx, dt.Id, dt.Name)
		if err != nil {
			return err
		}
	}

	for _, status := range data.Statuses {
		_, err = repository.CreateStatus(tx, status.Id, status.Name, status.ShortName, status.Color)
		if err != nil {
			return err
		}
	}

	for _, tag := range data.Tags {
		_, err = repository.CreateTag(tx, tag.Id, tag.Name)
		if err != nil {
			return err
		}
	}

	for _, collection := range data.Collections {
		err = repository.AddCollection(
			tx, collection.Id, collection.Name, collection.Description, collection.CollectionTypeId, collection.ParentId, collection.PreviewId, collection.IsLibrary)
		if err != nil {
			if err.Error() == "parent collection not found" {
				continue
			}
			return err
		}
	}

	for _, ca := range data.CollectionAssignees {
		err = repository.AddAssignee(tx, ca.Id, ca.CollectionId, ca.AssigneeId)
		if err != nil {
			return err
		}
	}

	createAssetStmt, err := tx.Prepare(`
		INSERT INTO asset 
		(id, assignee_id, mtime, created_at, name, description, extension, asset_type_id, collection_id, is_resource, status_id, pointer, is_link, preview_id) 
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);
	`)
	if err != nil {
		return err
	}
	for _, asset := range data.Assets {
		_, err := createAssetStmt.Exec(asset.Id, asset.AssigneeId, asset.MTime, asset.CreatedAt, asset.Name, asset.Description, asset.Extension, asset.AssetTypeId, asset.CollectionId, asset.IsResource, asset.StatusId, asset.Pointer, asset.IsLink, asset.PreviewId)
		if err != nil {
			return err
		}
	}

	createCheckpointStmt, err := tx.Prepare(`
		INSERT INTO asset_checkpoint 
		(id, mtime, created_at, asset_id, xxhash_checksum, time_modified, file_size, comment, chunks, author_id, preview_id, group_id) 
		VALUES (?, ?,?,?,?,?,?,?,?,?,?,?);
	`)
	if err != nil {
		return err
	}
	for _, cp := range data.AssetCheckpoints {
		epochTime, err := utils.RFC3339ToEpoch(cp.CreatedAt)
		if err != nil {
			return err
		}
		_, err = createCheckpointStmt.Exec(cp.Id, cp.MTime, epochTime, cp.AssetId, cp.XXHashChecksum, cp.TimeModified, cp.FileSize, cp.Comment, cp.Chunks, cp.AuthorUID, cp.PreviewId, cp.GroupId)
		if err != nil {
			return err
		}
	}

	for _, dep := range data.AssetDependencies {
		_, err = repository.AddDependency(tx, dep.Id, dep.AssetId, dep.DependencyId, dep.DependencyTypeId)
		if err != nil {
			if err.Error() == "UNIQUE constraint failed: asset_dependency.asset_id, asset_dependency.dependency_id" {
				continue
			}
			return err
		}
	}

	for _, dep := range data.CollectionDependencies {
		_, err = repository.AddCollectionDependency(tx, dep.Id, dep.AssetId, dep.DependencyId, dep.DependencyTypeId)
		if err != nil {
			if err.Error() == "UNIQUE constraint failed: collection_dependency.asset_id, collection_dependency.dependency_id" {
				continue
			}
			return err
		}
	}

	for _, template := range data.Templates {
		_, err = repository.AddTemplate(tx, template.Id, template.Name, template.Extension, template.Chunks, template.XxhashChecksum, template.FileSize)
		if err != nil {
			return err
		}
	}

	for _, workflow := range data.Workflows {
		_, err = repository.CreateWorkflow(tx, workflow.Id, workflow.Name, []models.WorkflowAsset{}, []models.WorkflowCollection{}, []models.WorkflowLink{})
		if err != nil {
			return err
		}
	}

	for _, wl := range data.WorkflowLinks {
		err = repository.AddLinkWorkflow(tx, wl.Id, wl.Name, wl.CollectionTypeId, wl.WorkflowId, wl.LinkedWorkflowId)
		if err != nil {
			return err
		}
	}

	for _, wc := range data.WorkflowCollections {
		_, err = repository.CreateWorkflowCollection(tx, wc.Id, wc.Name, wc.WorkflowId, wc.CollectionTypeId)
		if err != nil {
			return err
		}
	}

	for _, wa := range data.WorkflowAssets {
		_, err = repository.CreateWorkflowAsset(tx, wa.Id, wa.Name, wa.WorkflowId, wa.AssetTypeId, wa.IsResource, wa.TemplateId, wa.Pointer, wa.IsLink)
		if err != nil {
			return err
		}
	}

	for _, assetTag := range data.AssetTags {
		err = repository.AddTagToAssetById(tx, assetTag.Id, assetTag.AssetId, assetTag.TagId)
		if err != nil {
			return err
		}
	}

	// Integration project
	for _, ip := range data.IntegrationProjects {
		_, err = repository.CreateIntegrationProject(
			tx, ip.Id, ip.IntegrationId, ip.ExternalProjectId,
			ip.ExternalProjectName, ip.ApiUrl, ip.SyncOptions,
			ip.LinkedByUserId, ip.LinkedAt)
		if err != nil {
			return err
		}
	}

	// Integration collection mappings
	for _, mapping := range data.IntegrationCollectionMappings {
		_, err = repository.CreateCollectionMapping(
			tx, mapping.Id, mapping.IntegrationId, mapping.ExternalId, mapping.ExternalType,
			mapping.ExternalName, mapping.ExternalParentId, mapping.ExternalPath,
			mapping.ExternalMetadata, mapping.CollectionId, mapping.SyncedAt)
		if err != nil {
			return err
		}
	}

	// Integration asset mappings
	for _, mapping := range data.IntegrationAssetMappings {
		_, err = repository.CreateAssetMapping(
			tx, mapping.Id, mapping.IntegrationId, mapping.ExternalId, mapping.ExternalName,
			mapping.ExternalParentId, mapping.ExternalType, mapping.ExternalStatus,
			mapping.ExternalAssignees, mapping.ExternalMetadata, mapping.AssetId, mapping.SyncedAt)
		if err != nil {
			return err
		}
	}

	return nil
}

// FetchData retrieves project data from a remote server or local database file.
func FetchData(remoteUrl string, userId string, prepareRequest repository.PrepareRequest) (ProjectData, error) {
	userData := ProjectData{}
	userDataPb := repositorypb.ProjectData{}
	if utils.IsValidURL(remoteUrl) {
		type userTokenStruct struct {
			UserId string `json:"user_id"`
		}
		dataUrl := remoteUrl + "/data"

		userToken := userTokenStruct{UserId: userId}
		jsonData, err := json.Marshal(userToken)
		if err != nil {
			return userData, err
		}

		req, err := http.NewRequest("GET", dataUrl, bytes.NewBuffer(jsonData))
		if err != nil {
			return userData, err
		}
		req.Header.Set("Clustta-Agent", constants.USER_AGENT)
		prepareRequest(req)

		client := &http.Client{}
		response, err := client.Do(req)
		if err != nil {
			return userData, err
		}
		defer response.Body.Close()

		if response.StatusCode == 200 {
			body, err := io.ReadAll(response.Body)
			if err != nil {
				return userData, fmt.Errorf("error reading response body: %s", err.Error())
			}

			decompressedData, err := zstd.Decompress(nil, body)
			if err != nil {
				return userData, err
			}

			err = proto.Unmarshal(decompressedData, &userDataPb)
			if err != nil {
				return userData, err
			}

			userData = ProjectData{
				ProjectPreview:      userDataPb.ProjectPreview,
				CollectionTypes:     repository.FromPbCollectionTypes(userDataPb.CollectionTypes),
				Collections:         repository.FromPbCollections(userDataPb.Collections),
				CollectionAssignees: repository.FromPbCollectionAssignees(userDataPb.CollectionAssignees),

				AssetTypes:             repository.FromPbAssetTypes(userDataPb.AssetTypes),
				Assets:                 repository.FromPbAssets(userDataPb.Assets),
				AssetCheckpoints:       repository.FromPbCheckpoints(userDataPb.AssetCheckpoints),
				AssetDependencies:      repository.FromPbAssetDependencies(userDataPb.AssetDependencies),
				CollectionDependencies: repository.FromPbCollectionDependencies(userDataPb.CollectionDependencies),

				Statuses:        repository.FromPbStatuses(userDataPb.Statuses),
				DependencyTypes: repository.FromPbDependencyTypes(userDataPb.DependencyTypes),

				Users: repository.FromPbUsers(userDataPb.Users),
				Roles: repository.FromPbRoles(userDataPb.Roles),

				Templates: repository.FromPbTemplates(userDataPb.Templates),

				Workflows:           repository.FromPbWorkflows(userDataPb.Workflows),
				WorkflowLinks:       repository.FromPbWorkflowLinks(userDataPb.WorkflowLinks),
				WorkflowCollections: repository.FromPbWorkflowCollections(userDataPb.WorkflowCollections),
				WorkflowAssets:      repository.FromPbWorkflowAssets(userDataPb.WorkflowAssets),

				Tags:      repository.FromPbTags(userDataPb.Tags),
				AssetTags: repository.FromPbAssetTags(userDataPb.AssetTags),

				IntegrationProjects:           repository.FromPbIntegrationProjects(userDataPb.IntegrationProjects),
				IntegrationCollectionMappings: repository.FromPbIntegrationCollectionMappings(userDataPb.IntegrationCollectionMappings),
				IntegrationAssetMappings:      repository.FromPbIntegrationAssetMappings(userDataPb.IntegrationAssetMappings),
			}

			return userData, nil
		} else if response.StatusCode == 400 {
			body, err := io.ReadAll(response.Body)
			if err != nil {
				return userData, err
			}
			return userData, errors.New(string(body))
		}
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return userData, err
		}
		return userData, fmt.Errorf("unknown error while fetching data. url: %s, status code: %d, message: %s", dataUrl, response.StatusCode, string(body))
	} else if utils.FileExists(remoteUrl) {
		db, err := utils.OpenDb(remoteUrl)
		if err != nil {
			return userData, err
		}
		defer db.Close()
		remoteTx, err := db.Beginx()
		if err != nil {
			return userData, err
		}
		defer remoteTx.Rollback()
		userData, err = LoadUserData(remoteTx, userId)
		if err != nil {
			return userData, err
		}
	} else {
		return userData, fmt.Errorf("invalid url:%s", remoteUrl)
	}
	return userData, nil
}

// CalculateMissingPreviews returns preview IDs that are not yet in the local database.
func CalculateMissingPreviews(tx *sqlx.Tx, data ProjectData) ([]string, error) {
	previewIds := collectPreviewIds(data)
	return repository.GetNonExistingPreviews(tx, previewIds)
}

// CalculateMissingChunks determines which chunks are not yet downloaded locally.
func CalculateMissingChunks(tx *sqlx.Tx, data ProjectData, userId string, syncOptions SyncOptions) ([]string, []string, int, error) {
	assetsIds := []string{}

	for _, asset := range data.Assets {
		if asset.AssigneeId == userId {
			assetsIds = append(assetsIds, asset.Id)
		} else if syncOptions.AssetDependencies && asset.IsDependency {
			assetsIds = append(assetsIds, asset.Id)
		} else if syncOptions.Assets {
			assetsIds = append(assetsIds, asset.Id)
		}
	}

	var checkpointsToProcess []models.Checkpoint

	if syncOptions.OnlyLatestCheckpoints {
		latestAssetCheckpoints := make(map[string]models.Checkpoint)
		for _, cp := range data.AssetCheckpoints {
			if utils.Contains(assetsIds, cp.AssetId) {
				existing, found := latestAssetCheckpoints[cp.AssetId]
				if !found || cp.CreatedAt > existing.CreatedAt {
					latestAssetCheckpoints[cp.AssetId] = cp
				}
			}
		}
		for _, cp := range latestAssetCheckpoints {
			checkpointsToProcess = append(checkpointsToProcess, cp)
		}
	} else {
		for _, cp := range data.AssetCheckpoints {
			if utils.Contains(assetsIds, cp.AssetId) {
				checkpointsToProcess = append(checkpointsToProcess, cp)
			}
		}
	}

	seenChunks := make(map[string]bool)
	missingChunks := []string{}
	allChunks := []string{}
	totalSize := 0
	for _, cp := range checkpointsToProcess {
		chunkHashes := strings.Split(cp.Chunks, ",")
		checkpointFullyDownloaded := true
		for _, chunkHash := range chunkHashes {
			if repository.ChunkExists(chunkHash, tx, seenChunks) {
				continue
			}
			if !utils.Contains(missingChunks, chunkHash) {
				checkpointFullyDownloaded = false
				missingChunks = append(missingChunks, chunkHash)
			}
		}
		if !checkpointFullyDownloaded {
			totalSize += cp.FileSize
			allChunks = append(allChunks, chunkHashes...)
		}
	}

	for _, template := range data.Templates {
		chunkHashes := strings.Split(template.Chunks, ",")
		templateFullyDownloaded := true
		for _, chunkHash := range chunkHashes {
			if repository.ChunkExists(chunkHash, tx, seenChunks) {
				continue
			}
			if !utils.Contains(missingChunks, chunkHash) {
				templateFullyDownloaded = false
				missingChunks = append(missingChunks, chunkHash)
			}
		}
		if !templateFullyDownloaded {
			totalSize += template.FileSize
			allChunks = append(allChunks, chunkHashes...)
		}
	}

	return missingChunks, allChunks, totalSize, nil
}

// CalculateCheckpointsMissingChunks determines which chunks are missing for a set of checkpoints.
func CalculateCheckpointsMissingChunks(tx *sqlx.Tx, checkpoints []models.Checkpoint) ([]string, []string, int, error) {
	seenChunks := make(map[string]bool)
	missingChunks := []string{}
	allChunks := []string{}
	totalSize := 0
	for _, cp := range checkpoints {
		chunkHashes := strings.Split(cp.Chunks, ",")
		checkpointFullyDownloaded := true
		for _, chunkHash := range chunkHashes {
			if repository.ChunkExists(chunkHash, tx, seenChunks) {
				continue
			}
			if !utils.Contains(missingChunks, chunkHash) {
				checkpointFullyDownloaded = false
				missingChunks = append(missingChunks, chunkHash)
			}
		}
		if !checkpointFullyDownloaded {
			totalSize += cp.FileSize
			allChunks = append(allChunks, chunkHashes...)
		}
	}
	return missingChunks, allChunks, totalSize, nil
}

// IsUnsynced returns true if there is any unsynced data in the project.
func IsUnsynced(tx *sqlx.Tx) (bool, error) {
	data, err := LoadChangedData(tx)
	if err != nil {
		return false, err
	}
	return !data.IsEmpty(), nil
}

// FetchMissingChunks asks the remote which chunks it's missing from a given list.
func FetchMissingChunks(remoteUrl string, userId string, chunks []string, prepareRequest repository.PrepareRequest) ([]string, error) {
	if utils.IsValidURL(remoteUrl) {
		dataUrl := remoteUrl + "/chunks-missing"

		jsonData, err := json.Marshal(chunks)
		if err != nil {
			return []string{}, err
		}

		req, err := http.NewRequest("GET", dataUrl, bytes.NewBuffer(jsonData))
		if err != nil {
			return []string{}, err
		}
		req.Header.Set("Clustta-Agent", constants.USER_AGENT)
		prepareRequest(req)

		client := &http.Client{}
		response, err := client.Do(req)
		if err != nil {
			return []string{}, err
		}
		defer response.Body.Close()

		if response.StatusCode == 200 {
			body, err := io.ReadAll(response.Body)
			if err != nil {
				return []string{}, fmt.Errorf("error reading response body: %s", err.Error())
			}
			responseData := []string{}
			err = json.Unmarshal(body, &responseData)
			if err != nil {
				return []string{}, err
			}
			return responseData, nil
		}
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return []string{}, err
		}
		return []string{}, errors.New(string(body))
	} else if utils.FileExists(remoteUrl) {
		dbConn, err := utils.OpenDb(remoteUrl)
		if err != nil {
			return []string{}, err
		}
		defer dbConn.Close()
		remoteTx, err := dbConn.Beginx()
		if err != nil {
			return []string{}, err
		}
		defer remoteTx.Rollback()

		missingChunks := []string{}
		seenChunks := make(map[string]bool)
		for _, chunkHash := range chunks {
			if repository.ChunkExists(chunkHash, remoteTx, seenChunks) {
				continue
			}
			missingChunks = append(missingChunks, chunkHash)
		}
		return missingChunks, nil
	}
	return []string{}, fmt.Errorf("invalid url:%s", remoteUrl)
}

// FetchMissingPreviews asks the remote which previews are missing from a given list.
func FetchMissingPreviews(remoteUrl string, userId string, previews []string, prepareRequest repository.PrepareRequest) ([]string, error) {
	if utils.IsValidURL(remoteUrl) {
		dataUrl := remoteUrl + "/previews-exist"

		jsonData, err := json.Marshal(previews)
		if err != nil {
			return []string{}, err
		}

		req, err := http.NewRequest("GET", dataUrl, bytes.NewBuffer(jsonData))
		if err != nil {
			return []string{}, err
		}
		req.Header.Set("Clustta-Agent", constants.USER_AGENT)
		prepareRequest(req)

		client := &http.Client{}
		response, err := client.Do(req)
		if err != nil {
			return []string{}, err
		}
		defer response.Body.Close()

		if response.StatusCode == 200 {
			body, err := io.ReadAll(response.Body)
			if err != nil {
				return []string{}, fmt.Errorf("error reading response body: %s", err.Error())
			}
			responseData := []string{}
			err = json.Unmarshal(body, &responseData)
			if err != nil {
				return []string{}, err
			}
			return responseData, nil
		}
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return []string{}, err
		}
		return []string{}, errors.New(string(body))
	} else if utils.FileExists(remoteUrl) {
		dbConn, err := utils.OpenDb(remoteUrl)
		if err != nil {
			return []string{}, err
		}
		defer dbConn.Close()
		remoteTx, err := dbConn.Beginx()
		if err != nil {
			return []string{}, err
		}
		defer remoteTx.Rollback()

		missingPreviews := []string{}
		for _, previewHash := range previews {
			if repository.PreviewExists(previewHash, remoteTx) {
				continue
			}
			missingPreviews = append(missingPreviews, previewHash)
		}
		return missingPreviews, nil
	}
	return []string{}, fmt.Errorf("invalid url:%s", remoteUrl)
}

// SyncData fetches data from a remote and overwrites the local project.
func SyncData(workingData, remoteUrl string, userId string, prepareRequest repository.PrepareRequest, chunkProvider repository.ChunkProvider) error {
	dbConn, err := utils.OpenDb(workingData)
	if err != nil {
		return err
	}
	defer dbConn.Close()
	tx, err := dbConn.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	data, err := FetchData(remoteUrl, userId, prepareRequest)
	if err != nil {
		return err
	}
	err = ClearLocalData(tx)
	if err != nil {
		return err
	}
	err = WriteProjectData(tx, data, false, chunkProvider)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// collectPreviewIds gathers all unique preview IDs referenced in the project data.
func collectPreviewIds(data ProjectData) []string {
	previewIds := []string{}
	if data.ProjectPreview != "" && !utils.Contains(previewIds, data.ProjectPreview) {
		previewIds = append(previewIds, data.ProjectPreview)
	}
	for _, asset := range data.Assets {
		if asset.PreviewId != "" && !utils.Contains(previewIds, asset.PreviewId) {
			previewIds = append(previewIds, asset.PreviewId)
		}
	}
	for _, collection := range data.Collections {
		if collection.PreviewId != "" && !utils.Contains(previewIds, collection.PreviewId) {
			previewIds = append(previewIds, collection.PreviewId)
		}
	}
	for _, cp := range data.AssetCheckpoints {
		if cp.PreviewId != "" && !utils.Contains(previewIds, cp.PreviewId) {
			previewIds = append(previewIds, cp.PreviewId)
		}
	}
	return previewIds
}

// roleAttributesFromRole extracts RoleAttributes from a Role struct.
func roleAttributesFromRole(role models.Role) models.RoleAttributes {
	return models.RoleAttributes{
		ViewCollection:   role.ViewCollection,
		CreateCollection: role.CreateCollection,
		UpdateCollection: role.UpdateCollection,
		DeleteCollection: role.DeleteCollection,

		ViewAsset:   role.ViewAsset,
		CreateAsset: role.CreateAsset,
		UpdateAsset: role.UpdateAsset,
		DeleteAsset: role.DeleteAsset,

		ViewTemplate:   role.ViewTemplate,
		CreateTemplate: role.CreateTemplate,
		UpdateTemplate: role.UpdateTemplate,
		DeleteTemplate: role.DeleteTemplate,

		ViewCheckpoint:   role.ViewCheckpoint,
		CreateCheckpoint: role.CreateCheckpoint,
		DeleteCheckpoint: role.DeleteCheckpoint,

		PullChunk: role.PullChunk,

		AssignAsset:   role.AssignAsset,
		UnassignAsset: role.UnassignAsset,

		AddUser:    role.AddUser,
		RemoveUser: role.RemoveUser,
		ChangeRole: role.ChangeRole,

		ChangeStatus:   role.ChangeStatus,
		SetDoneAsset:   role.SetDoneAsset,
		SetRetakeAsset: role.SetRetakeAsset,

		ViewDoneAsset: role.ViewDoneAsset,

		ManageDependencies: role.ManageDependencies,
	}
}
