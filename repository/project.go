package repository

import (
	"bytes"
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/eaxum/clustta-core/auth"
	"github.com/eaxum/clustta-core/constants"
	error_service "github.com/eaxum/clustta-core/errors"
	"github.com/eaxum/clustta-core/repository/migrations"
	"github.com/eaxum/clustta-core/repository/models"
	"github.com/eaxum/clustta-core/utils"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
)

//go:embed template_files/*
var templateFS embed.FS

//go:embed schema.sql
var ProjectSchema string

type ProjectInfo struct {
	Id               string   `json:"id"`
	SyncToken        string   `json:"sync_token"`
	PreviewId        string   `json:"preview_id"`
	Name             string   `json:"name"`
	Icon             string   `json:"icon"`
	Version          float64  `json:"version"`
	Uri              string   `json:"uri"`
	WorkingDirectory string   `json:"working_directory"`
	LocationID       string   `json:"location_id,omitempty"`
	Remote           string   `json:"remote"`
	Valid            bool     `json:"valid"`
	Status           string   `json:"status"`
	HasRemote        bool     `json:"has_remote"`
	IsUnsynced       bool     `json:"is_unsynced"`
	IsDownloaded     bool     `json:"is_downloaded"`
	IsClosed         bool     `json:"is_closed"`
	IsOutdated       bool     `json:"is_outdated"`
	IsTracked        bool     `json:"is_tracked"`
	IsOffline        bool     `json:"is_offline"`
	IgnoreList       []string `json:"ignore_list"`
}

type ProjectConfig struct {
	Name  string      `json:"name" db:"name"`
	Value interface{} `json:"value" db:"value"`
	Mtime int         `json:"mtime" db:"mtime"`
}

// InitDB initializes a new project database with schema, default data, and initial user.
// The settingsProvider handles platform-specific directory resolution.
func InitDB(projectPath string, studioName, workingDir string, user auth.User, walMode bool, settingsProvider SettingsProvider) error {
	db, err := utils.OpenDb(projectPath)
	if err != nil {
		return err
	}
	defer db.Close()

	if walMode {
		_, err = db.Exec("PRAGMA journal_mode = WAL;")
		if err != nil {
			return err
		}
	}

	if !settingsProvider.IsServer() && workingDir == "" {
		projectName := strings.TrimSuffix(filepath.Base(projectPath), filepath.Ext(projectPath))
		workingDir, err = settingsProvider.GetDefaultWorkingDir(studioName, projectName)
		if err != nil {
			return err
		}
	}

	if !settingsProvider.IsServer() {
		if _, err := os.Stat(workingDir); os.IsNotExist(err) {
			err = os.MkdirAll(workingDir, os.ModePerm)
			if err != nil {
				return err
			}
		}
	}

	err = utils.CreateSchema(db, ProjectSchema)
	if err != nil {
		return err
	}

	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	project_id := uuid.New().String()
	_, err = tx.Exec("INSERT INTO config (name, value, mtime) VALUES ('project_id', ?, ?)", project_id, utils.GetEpochTime())
	if err != nil {
		return err
	}

	_, err = tx.Exec("INSERT INTO config (name, value, mtime) VALUES ('remote', ?, ?)", "", utils.GetEpochTime())
	if err != nil {
		return err
	}
	err = utils.SetStudioName(tx, studioName)
	if err != nil {
		return err
	}

	err = utils.SetProjectWorkingDir(tx, workingDir)
	if err != nil {
		return err
	}

	err = initData(tx)
	if err != nil {
		return err
	}

	role, err := GetRoleByName(tx, "admin")
	if err != nil {
		tx.Rollback()
		return err
	}
	_, err = AddKnownUser(tx, user.Id, user.Email, user.Username, user.FirstName, user.LastName, role.Id, []byte{}, true, &NoopAuthProvider{})
	if err != nil {
		return err
	}
	err = utils.SetProjectVersion(tx, migrations.LatestVersion)
	if err != nil {
		return err
	}
	tx.Commit()

	return nil
}

// NoopAuthProvider is a no-op auth provider used when auth operations
// are not needed (e.g., InitDB, WriteProjectData with fetchPhoto=false).
type NoopAuthProvider struct{}

func (n *NoopAuthProvider) GetActiveUser() (auth.User, error)   { return auth.User{}, nil }
func (n *NoopAuthProvider) GetToken() (auth.Token, error)       { return auth.Token{}, nil }
func (n *NoopAuthProvider) AttachBearerToken(req *http.Request) {}
func (n *NoopAuthProvider) FetchUserData(email string) (auth.User, error) {
	return auth.User{}, nil
}
func (n *NoopAuthProvider) FetchUserPhoto(userId string) ([]byte, error) { return []byte{}, nil }

func initData(tx *sqlx.Tx) error {
	_, err := GetOrCreateStatus(tx, "todo", "todo", "#c0c0c0")
	if err != nil {
		return err
	}
	_, err = GetOrCreateStatus(tx, "ready", "ready", "#f6a000")
	if err != nil {
		return err
	}
	_, err = GetOrCreateStatus(tx, "work in progress", "wip", "#7696ee")
	if err != nil {
		return err
	}
	_, err = GetOrCreateStatus(tx, "waiting for approval", "wfa", "#986dd1")
	if err != nil {
		return err
	}
	_, err = GetOrCreateStatus(tx, "retake", "retake", "#dd0620")
	if err != nil {
		return err
	}
	_, err = GetOrCreateStatus(tx, "done", "done", "#51e064")
	if err != nil {
		return err
	}

	_, err = GetOrCreateDependencyType(tx, "waiting on")
	if err != nil {
		return err
	}
	_, err = GetOrCreateDependencyType(tx, "blocking")
	if err != nil {
		return err
	}
	_, err = GetOrCreateDependencyType(tx, "linked")
	if err != nil {
		return err
	}
	_, err = GetOrCreateDependencyType(tx, "working")
	if err != nil {
		return err
	}

	adminRoleAttributes := models.RoleAttributes{
		ViewCollection:   true,
		CreateCollection: true,
		UpdateCollection: true,
		DeleteCollection: true,

		ViewAsset:   true,
		CreateAsset: true,
		UpdateAsset: true,
		DeleteAsset: true,

		ViewTemplate:   true,
		CreateTemplate: true,
		UpdateTemplate: true,
		DeleteTemplate: true,

		ViewCheckpoint:   true,
		CreateCheckpoint: true,
		DeleteCheckpoint: true,

		PullChunk: true,

		AssignAsset:   true,
		UnassignAsset: true,

		AddUser:    true,
		RemoveUser: true,
		ChangeRole: true,

		ChangeStatus:   true,
		SetDoneAsset:   true,
		SetRetakeAsset: true,

		ViewDoneAsset: true,

		ManageDependencies: true,
	}
	productionManagerRoleAttributes := models.RoleAttributes{
		ViewCollection:   true,
		CreateCollection: true,
		UpdateCollection: true,
		DeleteCollection: false,

		ViewAsset:   true,
		CreateAsset: true,
		UpdateAsset: true,
		DeleteAsset: false,

		ViewTemplate:   true,
		CreateTemplate: true,
		UpdateTemplate: true,
		DeleteTemplate: false,

		ViewCheckpoint:   true,
		CreateCheckpoint: false,
		DeleteCheckpoint: false,

		PullChunk: false,

		AssignAsset:   true,
		UnassignAsset: true,

		AddUser:    false,
		RemoveUser: false,
		ChangeRole: false,

		ChangeStatus:   true,
		SetDoneAsset:   true,
		SetRetakeAsset: true,

		ViewDoneAsset: true,

		ManageDependencies: true,
	}
	supervisorRoleAttributes := models.RoleAttributes{
		ViewCollection:   true,
		CreateCollection: false,
		UpdateCollection: false,
		DeleteCollection: false,

		ViewAsset:   true,
		CreateAsset: false,
		UpdateAsset: false,
		DeleteAsset: false,

		ViewTemplate:   false,
		CreateTemplate: false,
		UpdateTemplate: false,
		DeleteTemplate: false,

		ViewCheckpoint:   true,
		CreateCheckpoint: true,
		DeleteCheckpoint: true,

		PullChunk: true,

		AssignAsset:   true,
		UnassignAsset: true,

		AddUser:    false,
		RemoveUser: false,
		ChangeRole: false,

		ChangeStatus:   true,
		SetDoneAsset:   true,
		SetRetakeAsset: true,

		ViewDoneAsset: true,

		ManageDependencies: false,
	}
	assistantSupervisorRoleAttributes := models.RoleAttributes{
		ViewCollection:   false,
		CreateCollection: false,
		UpdateCollection: false,
		DeleteCollection: false,

		ViewAsset:   true,
		CreateAsset: false,
		UpdateAsset: false,
		DeleteAsset: false,

		ViewTemplate:   false,
		CreateTemplate: false,
		UpdateTemplate: false,
		DeleteTemplate: false,

		ViewCheckpoint:   true,
		CreateCheckpoint: true,
		DeleteCheckpoint: false,

		PullChunk: true,

		AssignAsset:   false,
		UnassignAsset: false,

		AddUser:    false,
		RemoveUser: false,
		ChangeRole: false,

		ChangeStatus:   true,
		SetDoneAsset:   true,
		SetRetakeAsset: true,

		ViewDoneAsset: true,

		ManageDependencies: false,
	}
	artistRoleAttributes := models.RoleAttributes{
		ViewCollection:   false,
		CreateCollection: false,
		UpdateCollection: false,
		DeleteCollection: false,

		ViewAsset:   false,
		CreateAsset: false,
		UpdateAsset: false,
		DeleteAsset: false,

		ViewTemplate:   false,
		CreateTemplate: false,
		UpdateTemplate: false,
		DeleteTemplate: false,

		ViewCheckpoint:   true,
		CreateCheckpoint: true,
		DeleteCheckpoint: false,

		PullChunk: true,

		AssignAsset:   false,
		UnassignAsset: false,

		AddUser:    false,
		RemoveUser: false,
		ChangeRole: false,

		ChangeStatus:   true,
		SetDoneAsset:   false,
		SetRetakeAsset: false,

		ViewDoneAsset: false,

		ManageDependencies: false,
	}
	vendorRoleAttributes := models.RoleAttributes{
		ViewCollection:   false,
		CreateCollection: false,
		UpdateCollection: false,
		DeleteCollection: false,

		ViewAsset:   false,
		CreateAsset: false,
		UpdateAsset: false,
		DeleteAsset: false,

		ViewTemplate:   false,
		CreateTemplate: false,
		UpdateTemplate: false,
		DeleteTemplate: false,

		ViewCheckpoint:   true,
		CreateCheckpoint: false,
		DeleteCheckpoint: false,

		PullChunk: true,

		AssignAsset:   false,
		UnassignAsset: false,

		AddUser:    false,
		RemoveUser: false,
		ChangeRole: false,

		ChangeStatus:   true,
		SetDoneAsset:   false,
		SetRetakeAsset: false,

		ViewDoneAsset: false,

		ManageDependencies: false,
	}
	_, err = GetOrCreateRole(tx, "admin", adminRoleAttributes)
	if err != nil {
		return err
	}
	_, err = GetOrCreateRole(tx, "production manager", productionManagerRoleAttributes)
	if err != nil {
		return err
	}
	_, err = GetOrCreateRole(tx, "supervisor", supervisorRoleAttributes)
	if err != nil {
		return err
	}
	_, err = GetOrCreateRole(tx, "assistant supervisor", assistantSupervisorRoleAttributes)
	if err != nil {
		return err
	}
	_, err = GetOrCreateRole(tx, "artist", artistRoleAttributes)
	if err != nil {
		return err
	}
	_, err = GetOrCreateRole(tx, "vendor", vendorRoleAttributes)
	if err != nil {
		return err
	}

	_, err = GetOrCreateAssetType(tx, "generic", "generic")
	if err != nil {
		return err
	}

	_, err = GetOrCreateAssetType(tx, "weblink", "website")
	if err != nil {
		return err
	}

	_, err = GetOrCreateCollectionType(tx, "generic", "folder")
	if err != nil {
		return err
	}
	return nil
}

func ClearTrash(tx *sqlx.Tx) error {
	deleteAssetAndCollections := `
		-- Delete asset_checkpoint records
		WITH RECURSIVE trashed_collections AS (
			SELECT id FROM collection WHERE trashed = 1
			UNION
			SELECT e.id FROM collection e
			INNER JOIN trashed_collections te ON e.parent_id = te.id
		)
		DELETE FROM asset_checkpoint 
		WHERE trashed = 1 
		OR asset_id IN (
			SELECT id FROM asset 
			WHERE trashed = 1 
			OR collection_id IN (SELECT id FROM trashed_collections)
		);

		-- Delete asset dependencies
		WITH RECURSIVE trashed_collections AS (
			SELECT id FROM collection WHERE trashed = 1
			UNION
			SELECT e.id FROM collection e
			INNER JOIN trashed_collections te ON e.parent_id = te.id
		)
		DELETE FROM asset_dependency 
		WHERE asset_id IN (
			SELECT id FROM asset 
			WHERE trashed = 1 
			OR collection_id IN (SELECT id FROM trashed_collections)
		)
		OR dependency_id IN (
			SELECT id FROM asset 
			WHERE trashed = 1 
			OR collection_id IN (SELECT id FROM trashed_collections)
		);

		-- Delete collection dependencies
		WITH RECURSIVE trashed_collections AS (
			SELECT id FROM collection WHERE trashed = 1
			UNION
			SELECT e.id FROM collection e
			INNER JOIN trashed_collections te ON e.parent_id = te.id
		)
		DELETE FROM collection_dependency 
		WHERE asset_id IN (
			SELECT id FROM asset 
			WHERE trashed = 1 
			OR collection_id IN (SELECT id FROM trashed_collections)
		)
		OR dependency_id IN (SELECT id FROM trashed_collections);

		-- Delete asset tags
		WITH RECURSIVE trashed_collections AS (
			SELECT id FROM collection WHERE trashed = 1
			UNION
			SELECT e.id FROM collection e
			INNER JOIN trashed_collections te ON e.parent_id = te.id
		)
		DELETE FROM asset_tag 
		WHERE asset_id IN (
			SELECT id FROM asset 
			WHERE trashed = 1 
			OR collection_id IN (SELECT id FROM trashed_collections)
		);

		-- Delete assets
		WITH RECURSIVE trashed_collections AS (
			SELECT id FROM collection WHERE trashed = 1
			UNION
			SELECT e.id FROM collection e
			INNER JOIN trashed_collections te ON e.parent_id = te.id
		)
		DELETE FROM asset 
		WHERE trashed = 1 
		OR collection_id IN (SELECT id FROM trashed_collections);

		-- Delete templates
		DELETE FROM template WHERE trashed = 1;

		-- Delete collections
		WITH RECURSIVE trashed_collections AS (
			SELECT id FROM collection WHERE trashed = 1
			UNION
			SELECT e.id FROM collection e
			INNER JOIN trashed_collections te ON e.parent_id = te.id
		)
		DELETE FROM collection WHERE id IN (SELECT id FROM trashed_collections);

		-- Clean up hanging references
		DELETE FROM asset WHERE collection_id != '' AND collection_id NOT IN (SELECT id FROM collection);
		DELETE FROM asset_checkpoint WHERE asset_id NOT IN (SELECT id FROM asset);
		DELETE FROM asset_dependency WHERE asset_id NOT IN (SELECT id FROM asset) OR dependency_id NOT IN (SELECT id FROM asset);
		DELETE FROM collection_dependency WHERE asset_id NOT IN (SELECT id FROM asset) OR dependency_id NOT IN (SELECT id FROM collection);
		DELETE FROM asset_tag WHERE asset_id NOT IN (SELECT id FROM asset) OR tag_id NOT IN (SELECT id FROM tag);
	`

	_, err := tx.Exec(deleteAssetAndCollections)
	if err != nil {
		return err
	}

	return nil
}

func Purge(projectPath string) error {
	dbConn, err := utils.OpenDb(projectPath)
	if err != nil {
		return err
	}
	defer dbConn.Close()
	tx, err := dbConn.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	err = ClearTrash(tx)
	if err != nil {
		return err
	}

	clearUnusedChunks := `
	WITH used_chunks AS (
		-- Chunks used in templates
		SELECT DISTINCT TRIM(value) as hash
		FROM template, json_each('["' || REPLACE(chunks, ',', '","') || '"]')
		WHERE chunks != ''
		UNION
		-- Chunks used in asset_checkpoints
		SELECT DISTINCT TRIM(value) as hash
		FROM asset_checkpoint, json_each('["' || REPLACE(chunks, ',', '","') || '"]')
		WHERE chunks != ''
	)
	DELETE FROM chunk 
	WHERE hash NOT IN (SELECT hash FROM used_chunks);
	`

	_, err = tx.Exec(clearUnusedChunks)
	if err != nil {
		tx.Rollback()
		return err
	}

	tx.Commit()

	return nil
}

// TrimProject clears all chunks from the database while retaining metadata.
// This is useful to free up space on remote projects that can re-fetch chunks when needed.
func TrimProject(projectPath string) error {
	dbConn, err := utils.OpenDb(projectPath)
	if err != nil {
		return err
	}
	defer dbConn.Close()

	tx, err := dbConn.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.Exec("DELETE FROM chunk")
	if err != nil {
		return err
	}

	_, err = tx.Exec("DELETE FROM preview")
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func Vacuum(projectPath string) error {
	dbConn, err := utils.OpenDb(projectPath)
	if err != nil {
		return err
	}

	_, err = dbConn.Exec("VACUUM")
	if err != nil {
		return err
	}

	return nil
}

func ClearProjectOrphans(projectPath string) error {
	db, err := utils.OpenDb(projectPath)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	query := `
		CREATE TEMPORARY TABLE IF NOT EXISTS temp_orphan_collections (id TEXT PRIMARY KEY);
		CREATE TEMPORARY TABLE IF NOT EXISTS temp_orphan_assets (id TEXT PRIMARY KEY);

		DELETE FROM temp_orphan_collections;
		DELETE FROM temp_orphan_assets;

		INSERT OR REPLACE INTO temp_orphan_collections
		WITH RECURSIVE orphan_collections AS (
			SELECT DISTINCT id
			FROM collection
			WHERE parent_id != '' 
			AND NOT EXISTS (SELECT 1 FROM collection parent WHERE parent.id = collection.parent_id)
			
			UNION
			
			SELECT DISTINCT e.id
			FROM collection e
			JOIN orphan_collections oe ON e.parent_id = oe.id
		)
		SELECT id FROM orphan_collections;

		INSERT OR REPLACE INTO temp_orphan_assets
		SELECT DISTINCT id
		FROM asset
		WHERE 
			(collection_id != '' AND NOT EXISTS (SELECT 1 FROM collection e WHERE e.id = collection_id))
			OR (collection_id IN (SELECT id FROM temp_orphan_collections));

		DELETE FROM asset_checkpoint
		WHERE asset_id IN (SELECT id FROM temp_orphan_assets);

		DELETE FROM asset_tag
		WHERE asset_id IN (SELECT id FROM temp_orphan_assets);

		DELETE FROM asset_dependency
		WHERE asset_id IN (SELECT id FROM temp_orphan_assets)
		OR dependency_id IN (SELECT id FROM temp_orphan_assets);

		DELETE FROM collection_dependency
		WHERE asset_id IN (SELECT id FROM temp_orphan_assets)
		OR dependency_id IN (SELECT id FROM temp_orphan_collections);

		DELETE FROM asset
		WHERE id IN (SELECT id FROM temp_orphan_assets);

		DELETE FROM collection
		WHERE id IN (SELECT id FROM temp_orphan_collections);

		DROP TABLE IF EXISTS temp_orphan_collections;
		DROP TABLE IF EXISTS temp_orphan_assets;
	`
	_, err = tx.Exec(query)
	if err != nil {
		return err
	}

	return nil
}

func VerifyProjectIntegrity(projectPath string) (bool, error) {
	db, err := utils.OpenDb(projectPath)
	if err != nil {
		return false, err
	}
	defer db.Close()

	tableChecks := [][2]string{
		{"config", "config"},
		{"template", "template"},
		{"tag", "tag"},
		{"status", "status"},
		{"collection", "entity"},
		{"collection_type", "entity_type"},
		{"asset", "task"},
		{"asset_type", "task_type"},
		{"dependency_type", "dependency_type"},
		{"asset_dependency", "task_dependency"},
		{"asset_tag", "task_tag"},
		{"asset_checkpoint", "task_checkpoint"},
		{"chunk", "chunk"},
		{"user", "user"},
	}
	for _, check := range tableChecks {
		newName, oldName := check[0], check[1]
		rows, err := db.Query("SELECT name FROM sqlite_master WHERE type='table' AND name IN (?, ?)", newName, oldName)
		if err != nil {
			return false, err
		}
		defer rows.Close()
		if !rows.Next() {
			return false, nil
		}
	}

	tx, err := db.Beginx()
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	statuses, err := GetStatuses(tx)
	if err != nil {
		return false, err
	}
	if len(statuses) < 3 {
		return false, errors.New("clst file missing data")
	}

	return true, nil
}

// UpdateProject runs all pending migrations on a project database.
func UpdateProject(projectPath string, migrationCtx *migrations.MigrationContext) error {
	db, err := utils.OpenDb(projectPath)
	if err != nil {
		return err
	}
	defer db.Close()

	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	projectVersion, err := utils.GetProjectVersion(tx)
	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Rollback()
	if err != nil {
		return err
	}

	return migrations.RunMigrations(db, projectVersion, ProjectSchema, migrationCtx)
}

// CreateProject creates a new project either remotely (via HTTP) or locally.
// The prepareRequest callback attaches auth tokens to HTTP requests.
// The settingsProvider handles platform-specific directory resolution.
// The authProvider is used for user data operations.
func CreateProject(projectUri, studioName, workingDir, templateName string, user auth.User, prepareRequest PrepareRequest, settingsProvider SettingsProvider, authProvider auth.Provider, chunkProvider ChunkProvider) (ProjectInfo, error) {
	projectInfo := ProjectInfo{}
	if templateName == "" {
		templateName = "No Template"
	}

	if utils.IsValidURL(projectUri) {
		fmt.Println(projectUri)
		req, err := http.NewRequest("POST", projectUri, nil)
		if err != nil {
			fmt.Println("POST:", err)
			return projectInfo, err
		}
		userJson, err := json.Marshal(user)
		if err != nil {
			fmt.Println("Marshal:", err)
			return projectInfo, err
		}
		req.Header.Set("UserData", string(userJson))
		req.Header.Set("UserId", user.Id)
		req.Header.Set("Clustta-Agent", constants.USER_AGENT)
		if prepareRequest != nil {
			prepareRequest(req)
		}

		client := &http.Client{}
		response, err := client.Do(req)
		fmt.Println(response)
		if err != nil {
			fmt.Println("Response Error:", err)
			return projectInfo, err
		}
		defer response.Body.Close()

		responseCode := response.StatusCode
		if responseCode == 200 {

			body, err := io.ReadAll(response.Body)
			if err != nil {
				return ProjectInfo{}, err
			}

			err = json.Unmarshal(body, &projectInfo)
			if err != nil {
				return projectInfo, err
			}

			projectInfo.HasRemote = false
			projectInfo.Uri = projectUri
			projectInfo.Remote = projectUri
			return projectInfo, nil
		} else {
			body, _ := io.ReadAll(response.Body)
			return projectInfo, fmt.Errorf("server error (%d): %s", responseCode, string(body))
		}
	} else {
		projectDir := filepath.Dir(projectUri)
		os.MkdirAll(projectDir, os.ModePerm)
		if utils.FileExists(projectUri) {
			verify, err := VerifyProjectIntegrity(projectUri)
			if err != nil {
				if err.Error() == "file is not a database" {
					return ProjectInfo{}, error_service.ErrInvalidProjectExists
				}
				return ProjectInfo{}, err
			}
			if verify {
				return ProjectInfo{}, error_service.ErrProjectExists
			}
			return ProjectInfo{}, error_service.ErrInvalidProjectExists
		}
		err := InitDB(projectUri, studioName, workingDir, user, false, settingsProvider)
		if err != nil {
			return ProjectInfo{}, err
		}

		if templateName != "No Template" {
			ProjectTemplatesPath, err := settingsProvider.GetUserProjectTemplatesPath()
			if err != nil {
				return ProjectInfo{}, err
			}
			templatePath := filepath.Join(ProjectTemplatesPath, templateName+".clst")

			err = LoadProjectTemplateData(projectUri, templatePath, chunkProvider)
			if err != nil {
				return ProjectInfo{}, err
			}
		}

		projectInfo, err := GetProjectInfo(projectUri, user, prepareRequest)
		if err != nil {
			return ProjectInfo{}, err
		}

		projectInfo.HasRemote = false
		projectInfo.Uri = projectUri
		projectInfo.Remote = projectUri
		projectInfo.IsDownloaded = true
		return projectInfo, nil
	}
}

// GetProjectInfo retrieves project information from a remote URL or local file.
// The prepareRequest callback attaches auth tokens to HTTP requests.
func GetProjectInfo(projectUri string, user auth.User, prepareRequest PrepareRequest) (ProjectInfo, error) {

	if utils.IsValidURL(projectUri) {
		projectUrl := projectUri
		req, err := http.NewRequest("GET", projectUrl, nil)
		if err != nil {
			return ProjectInfo{}, err
		}
		userJson, err := json.Marshal(user)
		if err != nil {
			return ProjectInfo{}, err
		}
		req.Header.Set("UserData", string(userJson))
		req.Header.Set("UserId", user.Id)
		req.Header.Set("Clustta-Agent", constants.USER_AGENT)
		if prepareRequest != nil {
			prepareRequest(req)
		}

		client := &http.Client{}
		response, err := client.Do(req)
		if err != nil {
			return ProjectInfo{}, err
		}
		defer response.Body.Close()

		if response.StatusCode != 200 {
			body, err := io.ReadAll(response.Body)
			if err != nil {
				return ProjectInfo{}, err
			}
			return ProjectInfo{}, errors.New(string(body))
		}

		body, err := io.ReadAll(response.Body)
		if err != nil {
			return ProjectInfo{}, err
		}
		projectInfo := ProjectInfo{}
		err = json.Unmarshal(body, &projectInfo)
		if err != nil {
			return projectInfo, err
		}
		return projectInfo, nil
	} else if utils.FileExists(projectUri) {
		absProjectPath, err := utils.ExpandPath(projectUri)
		if err != nil {
			return ProjectInfo{}, err
		}
		if !utils.FileExists(absProjectPath) {
			return ProjectInfo{}, error_service.ErrProjectNotFound
		}
		db, err := utils.OpenDb(absProjectPath)
		if err != nil {
			return ProjectInfo{}, err
		}
		defer db.Close()
		tx, err := db.Beginx()
		if err != nil {
			return ProjectInfo{}, err
		}
		defer tx.Rollback()

		projectName, err := utils.GetProjectName(tx)
		if err != nil {
			return ProjectInfo{}, err
		}
		projectVersion, err := utils.GetProjectVersion(tx)
		if err != nil {
			return ProjectInfo{}, err
		}
		isClosed, err := utils.GetIsClosed(tx)
		if err != nil {
			return ProjectInfo{}, err
		}
		workingDir, err := utils.GetProjectWorkingDir(tx)
		if err != nil {
			return ProjectInfo{}, err
		}
		projectId, err := utils.GetProjectId(tx)
		if err != nil {
			return ProjectInfo{}, err
		}
		projectPreview, err := GetProjectPreview(tx)
		if err != nil && err.Error() != "no preview" {
			return ProjectInfo{}, err
		}
		icon, err := utils.GetProjectIcon(tx)
		if err != nil {
			return ProjectInfo{}, err
		}
		ignoreList, err := GetIgnoreList(tx)
		if err != nil {
			return ProjectInfo{}, err
		}
		syncToken, err := utils.GetProjectSyncToken(tx)
		if err != nil {
			return ProjectInfo{}, err
		}
		remoteUrl, _ := utils.GetRemoteUrl(tx)
		hasRemote := remoteUrl != "" && utils.IsValidURL(remoteUrl)
		return ProjectInfo{
			Id:               projectId,
			SyncToken:        syncToken,
			PreviewId:        projectPreview.Hash,
			Name:             projectName,
			Icon:             icon,
			Version:          projectVersion,
			Remote:           remoteUrl,
			Uri:              absProjectPath,
			WorkingDirectory: workingDir,
			Status:           "normal",
			HasRemote:        hasRemote,
			IsClosed:         isClosed,
			IgnoreList:       ignoreList,
		}, nil
	} else {
		return ProjectInfo{}, fmt.Errorf("invalid url:%s", projectUri)
	}
}

// GetSyncToken retrieves the sync token from a remote URL or local project file.
// The prepareRequest callback attaches auth tokens to HTTP requests.
func GetSyncToken(projectUri string, user auth.User, prepareRequest PrepareRequest) (string, error) {
	if utils.IsValidURL(projectUri) {
		projectUrl := projectUri + "/sync-token"
		req, err := http.NewRequest("GET", projectUrl, nil)
		if err != nil {
			return "", err
		}
		userJson, err := json.Marshal(user)
		if err != nil {
			return "", err
		}
		req.Header.Set("UserData", string(userJson))
		req.Header.Set("UserId", user.Id)
		req.Header.Set("Clustta-Agent", constants.USER_AGENT)
		if prepareRequest != nil {
			prepareRequest(req)
		}

		client := &http.Client{}
		response, err := client.Do(req)
		if err != nil {
			return "", err
		}
		defer response.Body.Close()

		if response.StatusCode != 200 {
			body, err := io.ReadAll(response.Body)
			if err != nil {
				return "", err
			}
			return "", errors.New(string(body))
		}

		body, err := io.ReadAll(response.Body)
		if err != nil {
			return "", err
		}

		return string(body), nil
	} else if utils.FileExists(projectUri) {
		absProjectPath, err := utils.ExpandPath(projectUri)
		if err != nil {
			return "", err
		}
		if !utils.FileExists(absProjectPath) {
			return "", error_service.ErrProjectNotFound
		}
		db, err := utils.OpenDb(absProjectPath)
		if err != nil {
			return "", err
		}
		defer db.Close()
		tx, err := db.Beginx()
		if err != nil {
			return "", err
		}
		defer tx.Rollback()

		syncToken, err := utils.GetProjectSyncToken(tx)
		if err != nil {
			return "", err
		}
		return syncToken, nil
	} else {
		return "", fmt.Errorf("invalid url:%s", projectUri)
	}
}

func UserInProject(projectPath string, userId string) (bool, error) {
	db, err := utils.OpenDb(projectPath)
	if err != nil {
		return false, err
	}
	defer db.Close()
	tx, err := db.Beginx()
	defer tx.Rollback()
	if err != nil {
		return false, err
	}
	_, err = GetUser(tx, userId)
	if err != nil {
		if errors.Is(err, error_service.ErrUserNotFound) {
			return false, nil
		} else {
			return false, err
		}
	}
	return true, nil
}

// RenameProject renames a project either remotely (via HTTP) or locally.
// The prepareRequest callback attaches auth tokens to HTTP requests.
// The settingsProvider handles shared project directory resolution.
func RenameProject(projectUri, studioName, newName string, user auth.User, prepareRequest PrepareRequest, settingsProvider SettingsProvider) error {
	if utils.IsValidURL(projectUri) {
		data := []byte(fmt.Sprintf(`{"name": "%s"}`, newName))

		req, err := http.NewRequest(http.MethodPut, projectUri, bytes.NewBuffer(data))
		if err != nil {
			return err
		}

		userJson, err := json.Marshal(user)
		if err != nil {
			return err
		}
		req.Header.Set("UserData", string(userJson))
		req.Header.Set("UserId", user.Id)
		req.Header.Set("Clustta-Agent", constants.USER_AGENT)
		req.Header.Set("Content-Type", "application/json")
		if prepareRequest != nil {
			prepareRequest(req)
		}

		client := &http.Client{}
		response, err := client.Do(req)
		if err != nil {
			return err
		}
		defer response.Body.Close()

		if response.StatusCode != 200 {
			body, err := io.ReadAll(response.Body)
			if err != nil {
				return err
			}
			return errors.New(string(body))
		}

		sharedProjectsDir, err := settingsProvider.GetSharedProjectDirectory()
		if err != nil {
			return err
		}
		studioProjectsDir := filepath.Join(sharedProjectsDir, studioName)

		paths := strings.Split(projectUri, "/")

		oldProjectName := paths[len(paths)-1]
		oldProjectPath := filepath.Join(studioProjectsDir, oldProjectName+".clst")
		newProjectPath := filepath.Join(studioProjectsDir, newName+".clst")
		err = os.Rename(oldProjectPath, newProjectPath)
		if err != nil {
			return err
		}

		return nil
	} else {
		newProjectPath := filepath.Join(filepath.Dir(projectUri), newName+".clst")
		err := os.Rename(projectUri, newProjectPath)
		if err != nil {
			return err
		}
		return nil
	}

}

// SetIcon updates the project icon either remotely (via HTTP) or locally.
// The prepareRequest callback attaches auth tokens to HTTP requests.
// The settingsProvider handles shared project directory resolution.
func SetIcon(projectUri, studioName, icon string, user auth.User, prepareRequest PrepareRequest, settingsProvider SettingsProvider) error {
	if utils.IsValidURL(projectUri) {
		data := []byte(fmt.Sprintf(`{"icon": "%s"}`, icon))
		url := projectUri + "/icon"
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(data))
		if err != nil {
			return err
		}

		userJson, err := json.Marshal(user)
		if err != nil {
			return err
		}
		req.Header.Set("UserData", string(userJson))
		req.Header.Set("UserId", user.Id)
		req.Header.Set("Clustta-Agent", constants.USER_AGENT)
		req.Header.Set("Content-Type", "application/json")
		if prepareRequest != nil {
			prepareRequest(req)
		}

		client := &http.Client{}
		response, err := client.Do(req)
		if err != nil {
			return err
		}
		defer response.Body.Close()

		if response.StatusCode != 200 {
			body, err := io.ReadAll(response.Body)
			if err != nil {
				return err
			}
			return errors.New(string(body))
		}

		sharedProjectsDir, err := settingsProvider.GetSharedProjectDirectory()
		if err != nil {
			return err
		}
		studioProjectsDir := filepath.Join(sharedProjectsDir, studioName)

		paths := strings.Split(projectUri, "/")

		projectName := paths[len(paths)-1]
		projectPath := filepath.Join(studioProjectsDir, projectName+".clst")

		dbConn, err := utils.OpenDb(projectPath)
		if err != nil {
			return err
		}
		defer dbConn.Close()
		tx, err := dbConn.Beginx()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		err = utils.SetProjectIcon(tx, icon)
		if err != nil {
			return err
		}
		err = tx.Commit()
		if err != nil {
			return err
		}
		return nil
	} else {
		dbConn, err := utils.OpenDb(projectUri)
		if err != nil {
			return err
		}
		defer dbConn.Close()
		tx, err := dbConn.Beginx()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		err = utils.SetProjectIcon(tx, icon)
		if err != nil {
			return err
		}
		err = tx.Commit()
		if err != nil {
			return err
		}
		return nil
	}

}

// ToggleCloseProject toggles whether a project is closed.
// The prepareRequest callback attaches auth tokens to HTTP requests.
// The settingsProvider handles shared project directory resolution.
func ToggleCloseProject(projectUri, studioName string, user auth.User, prepareRequest PrepareRequest, settingsProvider SettingsProvider) error {
	if utils.IsValidURL(projectUri) {
		url := projectUri + "/toggle-close"
		req, err := http.NewRequest(http.MethodPut, url, nil)
		if err != nil {
			return err
		}

		userJson, err := json.Marshal(user)
		if err != nil {
			return err
		}
		req.Header.Set("UserData", string(userJson))
		req.Header.Set("UserId", user.Id)
		req.Header.Set("Clustta-Agent", constants.USER_AGENT)
		req.Header.Set("Content-Type", "application/json")
		if prepareRequest != nil {
			prepareRequest(req)
		}

		client := &http.Client{}
		response, err := client.Do(req)
		if err != nil {
			return err
		}
		defer response.Body.Close()

		if response.StatusCode != 200 {
			body, err := io.ReadAll(response.Body)
			if err != nil {
				return err
			}
			return errors.New(string(body))
		}

		sharedProjectsDir, err := settingsProvider.GetSharedProjectDirectory()
		if err != nil {
			return err
		}
		studioProjectsDir := filepath.Join(sharedProjectsDir, studioName)

		paths := strings.Split(projectUri, "/")
		projectName := paths[len(paths)-1]
		projectPath := filepath.Join(studioProjectsDir, projectName+".clst")

		if utils.FileExists(projectPath) {
			dbConn, err := utils.OpenDb(projectPath)
			if err != nil {
				return err
			}
			defer dbConn.Close()
			tx, err := dbConn.Beginx()
			if err != nil {
				return err
			}
			defer tx.Rollback()

			isClosed, err := utils.GetIsClosed(tx)
			if err != nil {
				return err
			}

			err = utils.SetIsClosed(tx, !isClosed)
			if err != nil {
				return err
			}
			err = tx.Commit()
			if err != nil {
				return err
			}
		}

		return nil
	} else {
		dbConn, err := utils.OpenDb(projectUri)
		if err != nil {
			return err
		}
		defer dbConn.Close()
		tx, err := dbConn.Beginx()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		isClosed, err := utils.GetIsClosed(tx)
		if err != nil {
			return err
		}

		err = utils.SetIsClosed(tx, !isClosed)
		if err != nil {
			return err
		}
		err = tx.Commit()
		if err != nil {
			return err
		}
		return nil
	}
}

// DeleteRemoteProject permanently deletes a project from the studio server.
// The prepareRequest callback attaches auth tokens to HTTP requests.
func DeleteRemoteProject(projectUri, studioName string, user auth.User, prepareRequest PrepareRequest) error {
	if !utils.IsValidURL(projectUri) {
		return errors.New("not a remote project URL")
	}

	req, err := http.NewRequest(http.MethodDelete, projectUri, nil)
	if err != nil {
		return err
	}

	userJson, err := json.Marshal(user)
	if err != nil {
		return err
	}
	req.Header.Set("UserData", string(userJson))
	req.Header.Set("UserId", user.Id)
	req.Header.Set("Clustta-Agent", constants.USER_AGENT)
	if prepareRequest != nil {
		prepareRequest(req)
	}

	client := &http.Client{}
	response, err := client.Do(req)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	if response.StatusCode != 200 {
		body, _ := io.ReadAll(response.Body)
		return errors.New(string(body))
	}

	return nil
}

// UpdateProjectWorkingDirectory updates the working directory path for a project.
// The settingsProvider handles project location management.
func UpdateProjectWorkingDirectory(projectUri, studioName, newWorkingDir string, user auth.User, settingsProvider SettingsProvider) error {
	if newWorkingDir == "" {
		return errors.New("working directory cannot be empty")
	}

	newWorkingDir = filepath.ToSlash(newWorkingDir)

	locations, err := settingsProvider.GetAllLocationPaths()
	if err != nil {
		return err
	}

	pathExists := false
	for _, loc := range locations {
		if strings.HasPrefix(newWorkingDir, loc.Path) {
			pathExists = true
			break
		}
	}

	if !pathExists {
		parentPath := filepath.Dir(newWorkingDir)
		locationName := filepath.Base(parentPath)
		if locationName == "." || locationName == "/" {
			locationName = "Custom Location"
		}

		_, err := settingsProvider.AddProjectLocation(locationName, parentPath)
		if err != nil {
			log.Printf("Warning: Could not add location to project locations: %v", err)
		}
	}

	if err := os.MkdirAll(newWorkingDir, os.ModePerm); err != nil {
		return fmt.Errorf("failed to create working directory: %w", err)
	}

	var projectPath string
	if utils.IsValidURL(projectUri) {
		sharedProjectsDir, err := settingsProvider.GetSharedProjectDirectory()
		if err != nil {
			return err
		}
		studioProjectsDir := filepath.Join(sharedProjectsDir, studioName)
		paths := strings.Split(projectUri, "/")
		projectName := paths[len(paths)-1]
		projectPath = filepath.Join(studioProjectsDir, projectName+".clst")

		if !utils.FileExists(projectPath) {
			return fmt.Errorf("local project file not found: %s", projectPath)
		}
	} else {
		projectPath = projectUri
	}

	dbConn, err := utils.OpenDb(projectPath)
	if err != nil {
		return err
	}
	defer dbConn.Close()

	tx, err := dbConn.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = utils.SetProjectWorkingDir(tx, newWorkingDir)
	if err != nil {
		return err
	}

	return tx.Commit()
}

func SetProjectPreview(tx *sqlx.Tx, previewPath string) error {
	preview, err := CreatePreview(tx, previewPath)
	if err != nil {
		return err
	}

	_, err = tx.Exec(`
		INSERT INTO config (name, value, mtime, synced)
		VALUES ('project_preview', $1, $2, 0)
		ON CONFLICT (name) DO UPDATE SET value = EXCLUDED.value, mtime = EXCLUDED.mtime, synced = 0
	`, preview.Hash, utils.GetEpochTime())
	if err != nil {
		return err
	}
	return nil
}

func GetProjectPreview(tx *sqlx.Tx) (models.Preview, error) {
	var previewHash string
	err := tx.Get(&previewHash, `
        SELECT value 
        FROM config 
        WHERE name = 'project_preview'
    `)
	if err != nil {
		if err == sql.ErrNoRows {
			return models.Preview{}, errors.New("no preview")
		}
		return models.Preview{}, err
	}
	preview, err := GetPreview(tx, previewHash)
	if err != nil {
		return models.Preview{}, err
	}
	return preview, nil
}

// SetIgnoreList sets the project's ignore list either remotely (via HTTP) or locally.
// The prepareRequest callback attaches auth tokens to HTTP requests.
// The settingsProvider handles shared project directory resolution.
func SetIgnoreList(projectUri, studioName string, ignoreList []string, user auth.User, prepareRequest PrepareRequest, settingsProvider SettingsProvider) error {
	if utils.IsValidURL(projectUri) {
		ignoreListJson, err := json.Marshal(ignoreList)
		if err != nil {
			return err
		}
		url := projectUri + "/ignore-list"
		req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(ignoreListJson))
		if err != nil {
			return err
		}

		userJson, err := json.Marshal(user)
		if err != nil {
			return err
		}
		req.Header.Set("UserData", string(userJson))
		req.Header.Set("UserId", user.Id)
		req.Header.Set("Clustta-Agent", constants.USER_AGENT)
		req.Header.Set("Content-Type", "application/json")
		if prepareRequest != nil {
			prepareRequest(req)
		}

		client := &http.Client{}
		response, err := client.Do(req)
		if err != nil {
			return err
		}
		defer response.Body.Close()

		if response.StatusCode != 200 {
			body, err := io.ReadAll(response.Body)
			if err != nil {
				return err
			}
			return errors.New(string(body))
		}

		sharedProjectsDir, err := settingsProvider.GetSharedProjectDirectory()
		if err != nil {
			return err
		}
		studioProjectsDir := filepath.Join(sharedProjectsDir, studioName)

		paths := strings.Split(projectUri, "/")

		projectName := paths[len(paths)-1]
		projectPath := filepath.Join(studioProjectsDir, projectName+".clst")

		dbConn, err := utils.OpenDb(projectPath)
		if err != nil {
			return err
		}
		defer dbConn.Close()
		tx, err := dbConn.Beginx()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		err = utils.SetProjectIgnoreList(tx, ignoreList)
		if err != nil {
			return err
		}
		err = tx.Commit()
		if err != nil {
			return err
		}
		return nil
	} else {
		dbConn, err := utils.OpenDb(projectUri)
		if err != nil {
			return err
		}
		defer dbConn.Close()
		tx, err := dbConn.Beginx()
		if err != nil {
			return err
		}
		defer tx.Rollback()

		err = utils.SetProjectIgnoreList(tx, ignoreList)
		if err != nil {
			return err
		}
		err = tx.Commit()
		if err != nil {
			return err
		}
		return nil
	}
}

func GetIgnoreList(tx *sqlx.Tx) ([]string, error) {
	var ignoreListJson string
	err := tx.Get(&ignoreListJson, `
		SELECT value 
		FROM config 
		WHERE name = 'ignore_list'
	`)
	if err != nil {
		if err == sql.ErrNoRows {
			return []string{}, nil
		}
		return []string{}, err
	}
	var ignoreList []string
	err = json.Unmarshal([]byte(ignoreListJson), &ignoreList)
	if err != nil {
		return []string{}, err
	}
	return ignoreList, nil
}

func IsProjectPreviewSynced(tx *sqlx.Tx) (bool, error) {
	var isSynced bool
	err := tx.Get(&isSynced, `
        SELECT synced 
        FROM config 
        WHERE name = 'project_preview'
    `)
	if err != nil {
		if err == sql.ErrNoRows {
			return true, nil
		}
		return false, err
	}
	return isSynced, nil
}

func SetProjectPreviewSynced(tx *sqlx.Tx) error {
	_, err := tx.Exec(`
        UPDATE SET synced = 1
        FROM config 
        WHERE name = 'project_preview'
    `)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return err
	}
	return nil
}

// TemplateData holds all data extracted from a project template.
type TemplateData struct {
	AssetTypes      []models.AssetType
	CollectionTypes []models.CollectionType
	IgnoreList      []string
	AssetTemplates  []models.Template
	ChunksInfo      []ChunkInfo
	AllChunkHashes  []string
}

// LoadProjectTemplateData loads template data into a newly created project.
// The chunkProvider handles chunk storage operations.
func LoadProjectTemplateData(projectPath, templatePath string, chunkProvider ChunkProvider) error {
	if !utils.FileExists(templatePath) {
		return fmt.Errorf("template not found: %s", templatePath)
	}

	templateData, err := extractTemplateData(templatePath, chunkProvider)
	if err != nil {
		return fmt.Errorf("failed to extract template data: %w", err)
	}

	missingChunks, err := copyTemplateMetadata(projectPath, templateData, chunkProvider)
	if err != nil {
		return fmt.Errorf("failed to copy template metadata: %w", err)
	}

	if len(missingChunks) > 0 {
		err = transferTemplateChunks(projectPath, templatePath, templateData.ChunksInfo, missingChunks, chunkProvider)
		if err != nil {
			return fmt.Errorf("failed to transfer chunks: %w", err)
		}
	}

	err = addTemplateDefinitions(projectPath, templateData.AssetTemplates)
	if err != nil {
		return fmt.Errorf("failed to add template definitions: %w", err)
	}

	return nil
}

// extractTemplateData reads all necessary data from the template database.
func extractTemplateData(templatePath string, chunkProvider ChunkProvider) (*TemplateData, error) {
	templateDbConn, err := utils.OpenDb(templatePath)
	if err != nil {
		return nil, err
	}
	defer templateDbConn.Close()

	templateTx, err := templateDbConn.Beginx()
	if err != nil {
		return nil, err
	}
	defer templateTx.Rollback()

	assetTypes, err := GetAssetTypes(templateTx)
	if err != nil {
		return nil, fmt.Errorf("failed to get asset types: %w", err)
	}

	collectionTypes, err := GetCollectionTypes(templateTx)
	if err != nil {
		return nil, fmt.Errorf("failed to get collection types: %w", err)
	}

	ignoreList, err := GetIgnoreList(templateTx)
	if err != nil {
		return nil, fmt.Errorf("failed to get ignore list: %w", err)
	}

	assetTemplates, err := GetTemplates(templateTx, false)
	if err != nil {
		return nil, fmt.Errorf("failed to get templates: %w", err)
	}

	allChunkHashes := collectChunkHashes(assetTemplates)

	chunksInfo, err := chunkProvider.GetChunksInfo(templateTx, allChunkHashes)
	if err != nil {
		return nil, fmt.Errorf("failed to get chunks info: %w", err)
	}

	return &TemplateData{
		AssetTypes:      assetTypes,
		CollectionTypes: collectionTypes,
		IgnoreList:      ignoreList,
		AssetTemplates:  assetTemplates,
		ChunksInfo:      chunksInfo,
		AllChunkHashes:  allChunkHashes,
	}, nil
}

// copyTemplateMetadata copies metadata from template to project and returns missing chunks.
func copyTemplateMetadata(projectPath string, data *TemplateData, chunkProvider ChunkProvider) ([]string, error) {
	projectDbConn, err := utils.OpenDb(projectPath)
	if err != nil {
		return nil, err
	}
	defer projectDbConn.Close()

	projectTx, err := projectDbConn.Beginx()
	if err != nil {
		return nil, err
	}
	defer projectTx.Rollback()

	err = utils.SetProjectIgnoreList(projectTx, data.IgnoreList)
	if err != nil {
		return nil, fmt.Errorf("failed to set ignore list: %w", err)
	}

	for _, assetType := range data.AssetTypes {
		_, err = GetOrCreateAssetType(projectTx, assetType.Name, assetType.Icon)
		if err != nil {
			if err.Error() == "UNIQUE constraint failed: asset_type.icon" {
				continue
			}
			return nil, fmt.Errorf("failed to create asset type %s: %w", assetType.Name, err)
		}
	}

	for _, collectionType := range data.CollectionTypes {
		_, err = GetOrCreateCollectionType(projectTx, collectionType.Name, collectionType.Icon)
		if err != nil {
			if err.Error() == "UNIQUE constraint failed: collection_type.icon" {
				continue
			}
			return nil, fmt.Errorf("failed to create collection type %s: %w", collectionType.Name, err)
		}
	}

	missingChunks, err := chunkProvider.GetNonExistingChunks(projectTx, data.AllChunkHashes)
	if err != nil {
		return nil, fmt.Errorf("failed to get non-existing chunks: %w", err)
	}

	err = projectTx.Commit()
	if err != nil {
		return nil, fmt.Errorf("failed to commit metadata: %w", err)
	}

	return missingChunks, nil
}

// transferTemplateChunks copies file chunks from template to project.
func transferTemplateChunks(projectPath, templatePath string, chunksInfo []ChunkInfo, missingChunks []string, chunkProvider ChunkProvider) error {
	err := chunkProvider.PullChunks(
		context.TODO(),
		projectPath,
		templatePath,
		chunksInfo,
		func(i1, i2 int, s1, s2 string) {},
	)
	if err != nil {
		return fmt.Errorf("chunk transfer failed: %w", err)
	}
	return nil
}

// addTemplateDefinitions adds template metadata to the project database.
func addTemplateDefinitions(projectPath string, assetTemplates []models.Template) error {
	projectDbConn, err := utils.OpenDb(projectPath)
	if err != nil {
		return err
	}
	defer projectDbConn.Close()

	projectTx, err := projectDbConn.Beginx()
	if err != nil {
		return err
	}
	defer projectTx.Rollback()

	for _, template := range assetTemplates {
		_, err := GetTemplateByName(projectTx, template.Name)
		if err == nil {
			continue
		}

		_, err = AddTemplate(
			projectTx,
			template.Id,
			template.Name,
			template.Extension,
			template.Chunks,
			template.XxhashChecksum,
			template.FileSize,
		)
		if err != nil {
			return fmt.Errorf("failed to add template %s: %w", template.Name, err)
		}
	}

	err = projectTx.Commit()
	if err != nil {
		return fmt.Errorf("failed to commit templates: %w", err)
	}

	return nil
}

// collectChunkHashes extracts all unique chunk hashes from a list of templates.
func collectChunkHashes(templates []models.Template) []string {
	chunkMap := make(map[string]bool)
	for _, template := range templates {
		if template.Chunks == "" {
			continue
		}
		chunkHashes := strings.Split(template.Chunks, ",")
		for _, hash := range chunkHashes {
			trimmedHash := strings.TrimSpace(hash)
			if trimmedHash != "" {
				chunkMap[trimmedHash] = true
			}
		}
	}

	chunks := make([]string, 0, len(chunkMap))
	for hash := range chunkMap {
		chunks = append(chunks, hash)
	}
	return chunks
}
