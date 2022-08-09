package session

import (
	"database/sql"
	"time"

	"github.com/cloudspannerecosystem/harbourbridge/internal"
)

type SchemaConversionSession struct {
	SessionMetadata
	VersionId              string
	PreviousVersionId      []string
	SchemaChanges          string
	SchemaConversionObject string
	CreateTimestamp        time.Time
}

type SessionMetadata struct {
	SessionName  string
	EditorName   string
	DatabaseType string
	DatabaseName string
	Notes        []string
	Tags         []string
}

type ConvWithMetadata struct {
	SessionMetadata
	internal.Conv
}

type SourceDBConnDetails struct {
	Host           string
	Port           string
	User           string
	Password       string
	Path           string
	ConnectionType string
}

// SessionState stores information for the current migration session.
type SessionState struct {
	SourceDB            *sql.DB             // Connection to source database in case of direct connection
	SourceDBConnDetails SourceDBConnDetails // Connection details for source database
	DbName              string              // Name of source database
	Driver              string              // Name of HarbourBridge driver in use
	Conv                *internal.Conv      // Current conversion state
	SessionFile         string              // Path to session file
	IsOffline           bool                // True if the connection to remote metadata database is invalid
	GCPProjectID        string
	SpannerInstanceID   string
	SessionMetadata     SessionMetadata
	Progress            *internal.Progress
	Error               error
	Counter
}

// Counter used to generate id for table, column, Foreignkey and indexes.
type Counter struct {
	ObjectId string
}
