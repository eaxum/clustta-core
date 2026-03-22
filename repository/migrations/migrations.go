package migrations

import (
	"github.com/eaxum/clustta-core/utils"

	"github.com/jmoiron/sqlx"
)

// LatestVersion is the current schema version after all migrations.
const LatestVersion = 1.9

// MigrationContext provides platform-specific functions needed by certain migrations.
type MigrationContext struct {
	// GetDefaultProjectWorkingDir returns the default working directory for a project.
	// Platform implementations resolve this using auth/settings as appropriate.
	GetDefaultProjectWorkingDir func(tx *sqlx.Tx) (string, error)
}

// Migration defines a single schema migration step.
type Migration struct {
	Version     float64
	Description string
	Up          func(db *sqlx.DB, schema string, ctx *MigrationContext) error
}

// All returns the ordered list of migrations.
func All() []Migration {
	return []Migration{
		{Version: 1.2, Description: "Rename checkpoint column, add columns, remap icons", Up: MigrateV1_2},
		{Version: 1.3, Description: "Set default working directory", Up: MigrateV1_3},
		{Version: 1.4, Description: "Add checkpoint grouping", Up: MigrateV1_4},
		{Version: 1.5, Description: "Add collection paths", Up: MigrateV1_5},
		{Version: 1.6, Description: "Add collection path update trigger", Up: MigrateV1_6},
		{Version: 1.7, Description: "Add integration tables", Up: MigrateV1_7},
		{Version: 1.8, Description: "Rename task/entity to asset/collection", Up: MigrateV1_8},
	}
}

// RunMigrations applies all pending migrations to the database.
func RunMigrations(db *sqlx.DB, currentVersion float64, schema string, ctx *MigrationContext) error {
	for _, m := range All() {
		shouldRun := false
		if m.Version == 1.2 {
			shouldRun = currentVersion == 1.2
		} else {
			shouldRun = currentVersion <= m.Version
		}

		if shouldRun {
			if err := m.Up(db, schema, ctx); err != nil {
				return err
			}
		}
	}

	err := utils.CreateSchema(db, schema)
	if err != nil {
		return err
	}

	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	err = utils.SetProjectVersion(tx, LatestVersion)
	if err != nil {
		return err
	}

	return tx.Commit()
}
