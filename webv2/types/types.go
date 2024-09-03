package types

import (
	"database/sql"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/profiles"
)

// TODO:(searce) organize this file according to go style guidelines: generally
// have public constants and public type definitions first, then public
// functions, and finally helper functions (usually in order of importance).

// driverConfig contains the parameters needed to make a direct database connection. It is
// used to communicate via HTTP with the frontend.
type DriverConfig struct {
	Driver      string `json:"Driver"`
	IsSharded   bool   `json:"IsSharded"`
	Host        string `json:"Host"`
	Port        string `json:"Port"`
	Database    string `json:"Database"`
	User        string `json:"User"`
	Password    string `json:"Password"`
	Dialect     string `json:"Dialect"`
	DataShardId string `json:"DataShardId"`
}

type DriverConfigs struct {
	DbConfigs         []DriverConfig `json:"DbConfigs"`
	IsRestoredSession string         `json:"IsRestoredSession"`
}

type ShardedDataflowConfig struct {
	MigrationProfile profiles.SourceProfileConfig
}

type SessionSummary struct {
	DatabaseType       string
	ConnectionDetail   string
	SourceTableCount   int
	SpannerTableCount  int
	SourceIndexCount   int
	SpannerIndexCount  int
	ConnectionType     string
	SourceDatabaseName string
	Region             string
	NodeCount          int
	ProcessingUnits    int
	Instance           string
	Dialect            string
	IsSharded          bool
}

type ProgressDetails struct {
	Progress       int
	ErrorMessage   string
	ProgressStatus int
}

type MigrationDetails struct {
	TargetDetails    TargetDetails             `json:"TargetDetails"`
	DatastreamConfig profiles.DatastreamConfig `json:"DatastreamConfig"`
	GcsConfig        profiles.GcsConfig        `json:"GcsConfig"`
	DataflowConfig   profiles.DataflowConfig   `json:"DataflowConfig"`
	MigrationMode    string                    `json:"MigrationMode"`
	MigrationType    string                    `json:"MigrationType"`
	IsSharded        bool                      `json:"IsSharded"`
	SkipForeignKeys  bool                      `json:"skipForeignKeys"`
}

type TargetDetails struct {
	TargetDB                    string          `json:"TargetDB"`
	SourceConnectionProfileName string          `json:"SourceConnProfile"`
	TargetConnectionProfileName string          `json:"TargetConnProfile"`
	ReplicationSlot             string          `json:"ReplicationSlot"`
	Publication                 string          `json:"Publication"`
	GcsMetadataPath             GcsMetadataPath `json:"GcsMetadataPath"`
}

type GcsMetadataPath struct {
	GcsBucketName     string `json:"GcsBucketName"`
	GcsBucketRootPath string `json:"GcsBucketRootPath"`
}

type ColMaxLength struct {
	SpDataType     string `json:"spDataType"`
	SpColMaxLength string `json:"spColMaxLength"`
}

type TableIdAndName struct {
	Id   string `json:"Id"`
	Name string `json:"Name"`
}

type ShardIdPrimaryKey struct {
	AddedAtTheStart bool `json:"AddedAtTheStart"`
}

// dumpConfig contains the parameters needed to run the tool using dump approach. It is
// used to communicate via HTTP with the frontend.
type DumpConfig struct {
	Driver   string `json:"Driver"`
	FilePath string `json:"Path"`
}

type SpannerDetails struct {
	Dialect string `json:"Dialect"`
}

type ConvertFromDumpRequest struct {
	Config         DumpConfig     `json:"Config"`
	SpannerDetails SpannerDetails `json:"SpannerDetails"`
}

// SessionState stores information for the current migration session.
type SessionState struct {
	sourceDB    *sql.DB        // Connection to source database in case of direct connection
	dbName      string         // Name of source database
	driver      string         // Name of Spanner migration tool driver in use
	conv        *internal.Conv // Current conversion state
	sessionFile string         // Path to session file
}

// Type and issue.
type TypeIssue struct {
	T        string
	Brief    string
	DisplayT string
}

type AutoGen struct {
	Name           string
	GenerationType string
}

type ResourceDetails struct {
	ResourceType string `json:"ResourceType"`
	ResourceName string `json:"ResourceName"`
	ResourceUrl  string `json:"ResourceUrl"`
	GcloudCmd    string `json:"GcloudCmd"`
}
type GeneratedResources struct {
	MigrationJobId string `json:"MigrationJobId"`
	DatabaseName   string `json:"DatabaseName"`
	DatabaseUrl    string `json:"DatabaseUrl"`
	BucketName     string `json:"BucketName"`
	BucketUrl      string `json:"BucketUrl"`
	//Used for single instance migration flow
	DataStreamJobName          string `json:"DataStreamJobName"`
	DataStreamJobUrl           string `json:"DataStreamJobUrl"`
	DataflowJobName            string `json:"DataflowJobName"`
	DataflowJobUrl             string `json:"DataflowJobUrl"`
	DataflowGcloudCmd          string `json:"DataflowGcloudCmd"`
	PubsubTopicName            string `json:"PubsubTopicName"`
	PubsubTopicUrl             string `json:"PubsubTopicUrl"`
	PubsubSubscriptionName     string `json:"PubsubSubscriptionName"`
	PubsubSubscriptionUrl      string `json:"PubsubSubscriptionUrl"`
	MonitoringDashboardName    string `json:"MonitoringDashboardName"`
	MonitoringDashboardUrl     string `json:"MonitoringDashboardUrl"`
	AggMonitoringDashboardName string `json:"AggMonitoringDashboardName"`
	AggMonitoringDashboardUrl  string `json:"AggMonitoringDashboardUrl"`
	//Used for sharded migration flow
	ShardToShardResourcesMap map[string][]ResourceDetails `json:"ShardToShardResourcesMap"`
}

type DropDetail struct {
	Name string `json:"Name"`
}

// TableInterleaveStatus stores data regarding interleave status.
type TableInterleaveStatus struct {
	Possible bool
	Parent   string
	OnDelete string
	Comment  string
}
