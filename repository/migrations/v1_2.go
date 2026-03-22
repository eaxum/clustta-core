package migrations

import (
	"github.com/eaxum/clustta-core/utils"

	"github.com/jmoiron/sqlx"
)

// MigrateV1_2 renames the checkpoint entity_id column, adds new columns,
// and remaps legacy icon names to new icon names.
func MigrateV1_2(db *sqlx.DB, _ string, _ *MigrationContext) error {
	err := utils.RenameColumn(db, "task_checkpoint", "entity_id", "task_id")
	if err != nil {
		return err
	}

	err = utils.AddColumnIfNotExist(db, "task", "is_resource", "BOOLEAN", "0", false)
	if err != nil {
		return err
	}

	err = utils.AddColumnIfNotExist(db, "config", "synced", "BOOLEAN", "0", false)
	if err != nil {
		return err
	}

	err = utils.AddColumnIfNotExist(db, "entity", "is_library", "BOOLEAN", "0", false)
	if err != nil {
		return err
	}

	iconMap := map[string]string{
		"hdri":                 "image",
		"character creation":   "masks",
		"prop creation":        "drum",
		"environment creation": "stall",
		"concept art":          "palette",
		"modeling":             "cube",
		"rigging":              "bone",
		"texturing":            "texture",
		"lookdev":              "mystery-ball",
		"editing":              "scissors",
		"previz":               "video-camera",
		"layout":               "shapes",
		"animation":            "man-running",
		"fx":                   "fire",
		"lighting":             "bulb",
		"rendering":            "camera-flash",
		"compositing":          "flow-chart",
		"character":            "masks",
		"prop":                 "drum",
		"environment":          "stall",
		"scene":                "tree",
		"shot":                 "clapboard",
		"sequence":             "film-strip",
		"episode":              "film-reel",
	}

	tx, err := db.Beginx()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	type typeRow struct {
		Id   string `db:"id"`
		Name string `db:"name"`
		Icon string `db:"icon"`
	}

	var taskTypes []typeRow
	if err := tx.Select(&taskTypes, "SELECT id, name, icon FROM task_type"); err == nil {
		for _, tt := range taskTypes {
			if newIcon, exists := iconMap[tt.Icon]; exists {
				_, _ = tx.Exec("UPDATE task_type SET icon = ? WHERE id = ?", newIcon, tt.Id)
			}
		}
	}

	var entityTypes []typeRow
	if err := tx.Select(&entityTypes, "SELECT id, name, icon FROM entity_type"); err == nil {
		for _, et := range entityTypes {
			if newIcon, exists := iconMap[et.Icon]; exists {
				_, _ = tx.Exec("UPDATE entity_type SET icon = ? WHERE id = ?", newIcon, et.Id)
			}
		}
	}

	return tx.Commit()
}
