package repository

import (
	"context"
	"net/http"

	"github.com/jmoiron/sqlx"
)

// LocationPath represents a file storage location.
type LocationPath struct {
	Name string
	Path string
}

// SettingsProvider defines the interface for platform-specific settings.
// Each repo (client, server, studio) implements this with its own logic.
type SettingsProvider interface {
	IsServer() bool
	GetDefaultWorkingDir(studioName, projectName string) (string, error)
	GetSharedProjectDirectory() (string, error)
	GetUserProjectTemplatesPath() (string, error)
	GetAllLocationPaths() ([]LocationPath, error)
	AddProjectLocation(name, path string) (LocationPath, error)
}

// ChunkProvider defines the interface for chunk storage operations.
// Each repo (client, server, studio) implements this with its own logic.
type ChunkProvider interface {
	GetChunksInfo(tx *sqlx.Tx, chunkHashes []string) ([]ChunkInfo, error)
	GetNonExistingChunks(tx *sqlx.Tx, chunks []string) ([]string, error)
	PullChunks(ctx context.Context, projectPath, remoteUrl string, chunkInfos []ChunkInfo, callback func(int, int, string, string)) error
}

// PrepareRequest is a function that modifies an HTTP request before sending.
// Used to attach auth tokens, user agent headers, etc.
type PrepareRequest func(req *http.Request)
