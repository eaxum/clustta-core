package sync_service

import (
	"fmt"

	"github.com/eaxum/clustta-core/repository"
	"github.com/eaxum/clustta-core/utils"
	"github.com/jmoiron/sqlx"
)

type SyncOptions struct {
	OnlyLatestCheckpoints bool `json:"only_latest_checkpoints"`
	AssetDependencies     bool `json:"asset_dependencies"`
	Assets                bool `json:"assets"`
	Resources             bool `json:"resources"`
	Templates             bool `json:"templates"`
	Force                 bool `json:"force"`
}

var ProjectTables = []string{
	"role", "user", "status", "tag",
	"asset_type", "asset", "dependency_type", "asset_dependency", "collection_dependency",
	"collection_type", "collection", "collection_assignee", "template",
	"workflow", "workflow_link", "workflow_collection", "workflow_asset",
	"asset_tag", "asset_checkpoint", "tomb",
	"integration_project", "integration_collection_mapping", "integration_asset_mapping",
}

func clearTables(tx *sqlx.Tx, tables []string) error {
	for _, table := range tables {
		query := "DELETE FROM " + table
		_, err := tx.Exec(query)
		if err != nil {
			return err
		}
	}
	return nil
}

func dropTables(tx *sqlx.Tx, tables []string) error {
	for _, table := range tables {
		query := "DROP TABLE IF EXISTS " + table
		_, err := tx.Exec(query)
		if err != nil {
			return fmt.Errorf("failed to drop table %s: %w", table, err)
		}
	}
	return nil
}

// ClearLocalData deletes all rows from the project tables.
func ClearLocalData(tx *sqlx.Tx) error {
	return clearTables(tx, ProjectTables)
}

// ClearLocalDataDrop drops all project tables and re-creates them from schema.
func ClearLocalDataDrop(tx *sqlx.Tx) error {
	err := dropTables(tx, ProjectTables)
	if err != nil {
		return err
	}
	return utils.CreateSchemaTx(tx, repository.ProjectSchema)
}
