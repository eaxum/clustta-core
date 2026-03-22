package sync_service

import (
	"database/sql"
	"fmt"
	"strings"

	base_service "github.com/eaxum/clustta-core/base_service"
	"github.com/eaxum/clustta-core/repository"
	"github.com/eaxum/clustta-core/repository/models"
	"github.com/eaxum/clustta-core/utils"
	"github.com/jmoiron/sqlx"
)

// LoadUserData loads all project data visible to a given user based on their role.
func LoadUserData(tx *sqlx.Tx, userId string) (ProjectData, error) {
	userData := ProjectData{}

	user, err := repository.GetUser(tx, userId)
	if err != nil {
		return ProjectData{}, err
	}
	userRole, err := repository.GetRole(tx, user.RoleId)
	if err != nil {
		return ProjectData{}, err
	}

	assets := []models.Asset{}
	if userRole.ViewAsset {
		assets, err = repository.GetAssets(tx, true)
		if err != nil {
			return ProjectData{}, err
		}
	} else {
		assets, err = repository.GetUserAssets(tx, user.Id)
		if err != nil {
			return ProjectData{}, err
		}
	}

	var assetIds []string
	for _, asset := range assets {
		assetIds = append(assetIds, asset.Id)
	}
	quotedAssetIds := make([]string, len(assetIds))
	for i, id := range assetIds {
		quotedAssetIds[i] = fmt.Sprintf("\"%s\"", id)
	}

	assetDependenciesQuery := fmt.Sprintf("SELECT * FROM asset_dependency WHERE asset_id IN (%s)", strings.Join(quotedAssetIds, ","))
	assetDependencies := []models.AssetDependency{}
	err = tx.Select(&assetDependencies, assetDependenciesQuery)
	if err != nil {
		return ProjectData{}, err
	}

	collectionDependenciesQuery := fmt.Sprintf("SELECT * FROM collection_dependency WHERE asset_id IN (%s)", strings.Join(quotedAssetIds, ","))
	collectionDependencies := []models.CollectionDependency{}
	err = tx.Select(&collectionDependencies, collectionDependenciesQuery)
	if err != nil {
		return ProjectData{}, err
	}

	var uniqueDependencyIds []string
	for _, dependency := range assetDependencies {
		if !utils.Contains(uniqueDependencyIds, dependency.DependencyId) && !utils.Contains(assetIds, dependency.DependencyId) {
			uniqueDependencyIds = append(uniqueDependencyIds, dependency.DependencyId)
		}
	}
	quotedUniqueDependencyIds := make([]string, len(uniqueDependencyIds))
	for i, id := range uniqueDependencyIds {
		quotedUniqueDependencyIds[i] = fmt.Sprintf("\"%s\"", id)
	}

	uniqueDependenciesQuery := fmt.Sprintf("SELECT * FROM asset WHERE trashed = 0 AND id IN (%s)", strings.Join(quotedUniqueDependencyIds, ","))
	uniqueDependencies := []models.Asset{}
	err = tx.Select(&uniqueDependencies, uniqueDependenciesQuery)
	if err != nil {
		return ProjectData{}, err
	}

	assets = append(assets, uniqueDependencies...)
	assetIds = append(assetIds, uniqueDependencyIds...)
	quotedAssetIds = append(quotedAssetIds, quotedUniqueDependencyIds...)

	checkpointQuery := fmt.Sprintf("SELECT * FROM asset_checkpoint WHERE trashed = 0 AND asset_id IN (%s)", strings.Join(quotedAssetIds, ","))
	assetsCheckpoints := []models.Checkpoint{}
	err = tx.Select(&assetsCheckpoints, checkpointQuery)
	if err != nil {
		return ProjectData{}, err
	}

	statuses, err := repository.GetStatuses(tx)
	if err != nil {
		return ProjectData{}, err
	}
	assetTypes, err := repository.GetAssetTypes(tx)
	if err != nil {
		return ProjectData{}, err
	}

	users, err := repository.GetUsers(tx)
	if err != nil {
		return ProjectData{}, err
	}
	roles, err := repository.GetRoles(tx)
	if err != nil {
		return ProjectData{}, err
	}

	dependencyTypes, err := repository.GetDependencyTypes(tx)
	if err != nil {
		return ProjectData{}, err
	}

	collectionTypes, err := repository.GetCollectionTypes(tx)
	if err != nil {
		return ProjectData{}, err
	}

	collections := []models.Collection{}
	collectionAssignees := []models.CollectionAssignee{}
	if userRole.ViewAsset {
		collections, err = repository.GetCollections(tx, true)
		if err != nil {
			return ProjectData{}, err
		}
		err = tx.Select(&collectionAssignees, "SELECT * FROM collection_assignee")
		if err != nil {
			return ProjectData{}, err
		}
	} else {
		collections, err = repository.GetUserCollections(tx, assets, user.Id)
		if err != nil {
			return ProjectData{}, err
		}
		qoutedCollectionIds := make([]string, len(collections))
		for i, collection := range collections {
			qoutedCollectionIds[i] = fmt.Sprintf("\"%s\"", collection.Id)
		}
		collectionAssigneesQuery := fmt.Sprintf("SELECT * FROM collection_assignee WHERE collection_id IN (%s)", strings.Join(qoutedCollectionIds, ","))
		err = tx.Select(&collectionAssignees, collectionAssigneesQuery)
		if err != nil {
			return ProjectData{}, err
		}
	}

	templates := []models.Template{}
	if userRole.CreateAsset {
		templates, err = repository.GetTemplates(tx, false)
		if err != nil {
			return ProjectData{}, err
		}
	}

	workflows := []models.Workflow{}
	if userRole.CreateAsset {
		workflows, err = repository.GetWorkflows(tx)
		if err != nil {
			return ProjectData{}, err
		}
	}
	workflowLinks := []models.WorkflowLink{}
	if userRole.CreateAsset {
		err = base_service.GetAll(tx, "workflow_link", &workflowLinks)
		if err != nil {
			return ProjectData{}, err
		}
	}
	workflowCollections := []models.WorkflowCollection{}
	if userRole.CreateAsset {
		err = base_service.GetAll(tx, "workflow_collection", &workflowCollections)
		if err != nil {
			return ProjectData{}, err
		}
	}
	workflowAssets := []models.WorkflowAsset{}
	if userRole.CreateAsset {
		err = base_service.GetAll(tx, "workflow_asset", &workflowAssets)
		if err != nil {
			return ProjectData{}, err
		}
	}

	tags, err := repository.GetTags(tx)
	if err != nil {
		return ProjectData{}, err
	}

	assetstagsQuery := fmt.Sprintf("SELECT * FROM asset_tag WHERE asset_id IN (%s)", strings.Join(quotedAssetIds, ","))
	assetsTags := []models.AssetTag{}
	err = tx.Select(&assetsTags, assetstagsQuery)
	if err != nil {
		return ProjectData{}, err
	}

	projectPreview, err := repository.GetProjectPreview(tx)
	if err != nil {
		if err.Error() != "no preview" {
			return ProjectData{}, err
		}
	}

	userData.ProjectPreview = projectPreview.Hash
	userData.CollectionTypes = collectionTypes
	userData.Collections = collections
	userData.CollectionAssignees = collectionAssignees

	userData.AssetTypes = assetTypes
	userData.Assets = assets
	userData.AssetCheckpoints = assetsCheckpoints
	userData.AssetDependencies = assetDependencies
	userData.CollectionDependencies = collectionDependencies

	userData.Statuses = statuses
	userData.DependencyTypes = dependencyTypes

	userData.Users = users
	userData.Roles = roles

	userData.Templates = templates

	userData.Workflows = workflows
	userData.WorkflowLinks = workflowLinks
	userData.WorkflowCollections = workflowCollections
	userData.WorkflowAssets = workflowAssets

	userData.Tags = tags
	userData.AssetTags = assetsTags
	return userData, nil
}

// LoadChangedData loads all unsynced data from the project database.
func LoadChangedData(tx *sqlx.Tx) (ProjectData, error) {
	userData := ProjectData{}

	assets := []models.Asset{}
	err := tx.Select(&assets, "SELECT * FROM asset WHERE synced = 0")
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}

	assetsCheckpoints := []models.Checkpoint{}
	err = tx.Select(&assetsCheckpoints, "SELECT * FROM asset_checkpoint WHERE synced = 0")
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}

	assetDependencies := []models.AssetDependency{}
	err = tx.Select(&assetDependencies, "SELECT * FROM asset_dependency WHERE synced = 0")
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}

	collectionDependencies := []models.CollectionDependency{}
	err = tx.Select(&collectionDependencies, "SELECT * FROM collection_dependency WHERE synced = 0")
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}

	statuses := []models.Status{}
	err = tx.Select(&statuses, "SELECT * FROM status WHERE synced = 0")
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}

	dependencyTypes := []models.DependencyType{}
	err = tx.Select(&dependencyTypes, "SELECT * FROM dependency_type WHERE synced = 0")
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}

	assetTypes := []models.AssetType{}
	err = tx.Select(&assetTypes, "SELECT * FROM asset_type WHERE synced = 0")
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}

	collectionTypes := []models.CollectionType{}
	err = tx.Select(&collectionTypes, "SELECT * FROM collection_type WHERE synced = 0")
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}

	collections := []models.Collection{}
	err = tx.Select(&collections, "SELECT * FROM collection WHERE synced = 0")
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}

	collectionAssignees := []models.CollectionAssignee{}
	err = tx.Select(&collectionAssignees, "SELECT * FROM collection_assignee WHERE synced = 0")
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}

	users := []models.User{}
	err = tx.Select(&users, "SELECT * FROM user WHERE synced = 0")
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}

	roles := []models.Role{}
	err = tx.Select(&roles, "SELECT * FROM role WHERE synced = 0")
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}

	templates := []models.Template{}
	err = tx.Select(&templates, "SELECT * FROM template WHERE synced = 0")
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}

	workflows := []models.Workflow{}
	err = tx.Select(&workflows, "SELECT * FROM workflow WHERE synced = 0")
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}
	workflowLinks := []models.WorkflowLink{}
	err = tx.Select(&workflowLinks, "SELECT * FROM workflow_link WHERE synced = 0")
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}
	workflowCollections := []models.WorkflowCollection{}
	err = tx.Select(&workflowCollections, "SELECT * FROM workflow_collection WHERE synced = 0")
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}
	workflowAssets := []models.WorkflowAsset{}
	err = tx.Select(&workflowAssets, "SELECT * FROM workflow_asset WHERE synced = 0")
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}

	tags := []models.Tag{}
	err = tx.Select(&tags, "SELECT * FROM tag WHERE synced = 0")
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}
	assetsTags := []models.AssetTag{}
	err = tx.Select(&assetsTags, "SELECT * FROM asset_tag WHERE synced = 0")
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}

	tombs, err := repository.GetTombs(tx)
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}

	isProjectPreviewSynced, err := repository.IsProjectPreviewSynced(tx)
	if err != nil {
		return userData, err
	}
	if !isProjectPreviewSynced {
		projectPreview, err := repository.GetProjectPreview(tx)
		if err != nil {
			return ProjectData{}, err
		}
		userData.ProjectPreview = projectPreview.Hash
	}

	userData.AssetTypes = assetTypes
	userData.Assets = assets
	userData.AssetCheckpoints = assetsCheckpoints
	userData.AssetDependencies = assetDependencies
	userData.CollectionDependencies = collectionDependencies

	userData.Statuses = statuses
	userData.DependencyTypes = dependencyTypes

	userData.Users = users
	userData.Roles = roles

	userData.CollectionTypes = collectionTypes
	userData.Collections = collections
	userData.CollectionAssignees = collectionAssignees

	userData.Templates = templates

	userData.Workflows = workflows
	userData.WorkflowLinks = workflowLinks
	userData.WorkflowCollections = workflowCollections
	userData.WorkflowAssets = workflowAssets

	userData.Tags = tags
	userData.AssetTags = assetsTags

	userData.Tombs = tombs

	// Integration tables
	integrationProjects := []models.IntegrationProject{}
	err = tx.Select(&integrationProjects, "SELECT * FROM integration_project WHERE synced = 0")
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}
	userData.IntegrationProjects = integrationProjects

	integrationCollectionMappings := []models.IntegrationCollectionMapping{}
	err = tx.Select(&integrationCollectionMappings, "SELECT * FROM integration_collection_mapping WHERE synced = 0")
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}
	userData.IntegrationCollectionMappings = integrationCollectionMappings

	integrationAssetMappings := []models.IntegrationAssetMapping{}
	err = tx.Select(&integrationAssetMappings, "SELECT * FROM integration_asset_mapping WHERE synced = 0")
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}
	userData.IntegrationAssetMappings = integrationAssetMappings

	return userData, nil
}

// LoadCheckpointData loads only unsynced checkpoints.
func LoadCheckpointData(tx *sqlx.Tx) (ProjectData, error) {
	userData := ProjectData{}
	assetsCheckpoints := []models.Checkpoint{}
	err := tx.Select(&assetsCheckpoints, "SELECT * FROM asset_checkpoint WHERE synced = 0")
	if err != nil && err != sql.ErrNoRows {
		return userData, err
	}
	userData.AssetCheckpoints = assetsCheckpoints
	return userData, nil
}

// LoadAssetData loads a single asset and its unsynced checkpoints.
func LoadAssetData(tx *sqlx.Tx, assetId string) (ProjectData, error) {
	data := ProjectData{}

	asset := models.Asset{}
	err := tx.Get(&asset, "SELECT * FROM asset WHERE id = ? AND synced = 0", assetId)
	if err != nil && err != sql.ErrNoRows {
		return data, err
	}
	if err == nil {
		data.Assets = []models.Asset{asset}
	}

	checkpoints := []models.Checkpoint{}
	err = tx.Select(&checkpoints, "SELECT * FROM asset_checkpoint WHERE asset_id = ? AND synced = 0", assetId)
	if err != nil && err != sql.ErrNoRows {
		return data, err
	}
	data.AssetCheckpoints = checkpoints

	return data, nil
}
