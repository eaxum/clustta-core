package migrations

import (
	"github.com/eaxum/clustta-core/utils"

	"github.com/jmoiron/sqlx"
)

// MigrateV1_7 re-applies the schema to add integration tables.
func MigrateV1_7(db *sqlx.DB, schema string, _ *MigrationContext) error {
	return utils.CreateSchema(db, schema)
}
