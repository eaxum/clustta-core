package migrations

import (
	"github.com/eaxum/clustta-core/utils"

	"github.com/jmoiron/sqlx"
)

// MigrateV1_3 sets the default working directory for the project.
// Uses MigrationContext.GetDefaultProjectWorkingDir for platform-specific resolution.
func MigrateV1_3(db *sqlx.DB, _ string, ctx *MigrationContext) error {
	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	projectWorkingDir := ""
	if ctx != nil && ctx.GetDefaultProjectWorkingDir != nil {
		projectWorkingDir, err = ctx.GetDefaultProjectWorkingDir(tx)
		if err != nil {
			return err
		}
	}

	err = utils.SetProjectWorkingDir(tx, projectWorkingDir)
	if err != nil {
		return err
	}

	return tx.Commit()
}
