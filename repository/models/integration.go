package models

// IntegrationProject stores the link between a Clustta project and an external integration.
type IntegrationProject struct {
	Id                  string `db:"id" json:"id"`
	MTime               int    `db:"mtime" json:"mtime"`
	IntegrationId       string `db:"integration_id" json:"integration_id"`
	ExternalProjectId   string `db:"external_project_id" json:"external_project_id"`
	ExternalProjectName string `db:"external_project_name" json:"external_project_name"`
	ApiUrl              string `db:"api_url" json:"api_url"`
	SyncOptions         string `db:"sync_options" json:"sync_options"`
	LinkedByUserId      string `db:"linked_by_user_id" json:"linked_by_user_id"`
	LinkedAt            string `db:"linked_at" json:"linked_at"`
	Enabled             bool   `db:"enabled" json:"enabled"`
	Synced              bool   `db:"synced" json:"synced"`
}

// IntegrationCollectionMapping maps external hierarchy items to Clustta Collections.
type IntegrationCollectionMapping struct {
	Id               string `db:"id" json:"id"`
	MTime            int    `db:"mtime" json:"mtime"`
	IntegrationId    string `db:"integration_id" json:"integration_id"`
	ExternalId       string `db:"external_id" json:"external_id"`
	ExternalType     string `db:"external_type" json:"external_type"`
	ExternalName     string `db:"external_name" json:"external_name"`
	ExternalParentId string `db:"external_parent_id" json:"external_parent_id"`
	ExternalPath     string `db:"external_path" json:"external_path"`
	ExternalMetadata string `db:"external_metadata" json:"external_metadata"`
	CollectionId     string `db:"collection_id" json:"collection_id"`
	SyncedAt         string `db:"synced_at" json:"synced_at"`
	Synced           bool   `db:"synced" json:"synced"`
}

// IntegrationAssetMapping maps external tasks to Clustta Assets.
type IntegrationAssetMapping struct {
	Id                     string `db:"id" json:"id"`
	MTime                  int    `db:"mtime" json:"mtime"`
	IntegrationId          string `db:"integration_id" json:"integration_id"`
	ExternalId             string `db:"external_id" json:"external_id"`
	ExternalName           string `db:"external_name" json:"external_name"`
	ExternalParentId       string `db:"external_parent_id" json:"external_parent_id"`
	ExternalType           string `db:"external_type" json:"external_type"`
	ExternalStatus         string `db:"external_status" json:"external_status"`
	ExternalAssignees      string `db:"external_assignees" json:"external_assignees"`
	ExternalMetadata       string `db:"external_metadata" json:"external_metadata"`
	AssetId                string `db:"asset_id" json:"asset_id"`
	LastPushedCheckpointId string `db:"last_pushed_checkpoint_id" json:"last_pushed_checkpoint_id"`
	SyncedAt               string `db:"synced_at" json:"synced_at"`
	Synced                 bool   `db:"synced" json:"synced"`
}
