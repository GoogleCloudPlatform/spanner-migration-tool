export enum InputType {
  DirectConnect = 'directConnect',
  DumpFile = 'dumpFile',
  SessionFile = 'sessionFile',
  ResumeSession = 'resumeSession',
}

export enum StorageKeys {
  Type = 'inputType',
  Config = 'config',
  SourceDbName = 'sourceDbName',
}

export enum SourceDbNames {
  MySQL = 'MySQL',
  Postgres = 'Postgres',
  SQLServer = 'SQL Server',
  Oracle = 'Oracle',
}

export enum ObjectExplorerNodeType {
  DbName = 'databaseName',
  Tables = 'tables',
  Table = 'tableName',
  Indexes = 'indexes',
  Index = 'indexName',
}

export enum RulesTypes {
  ChangeGlobalDataType = 'globalDatatype',
  AddIndex = 'addIndex',
}

export enum MigrationModes {
  schemaOnly = 'Schema',
  dataOnly = 'Data',
  schemaAndData = 'Schema And Data',
}

export enum ObjectDetailNodeType {
  Table = 'table',
  Index = 'index',
}

export enum MigrationTypes {
  bulkMigration = 'bulk',
  lowDowntimeMigration = 'lowdt'
}

export enum MigrationDetails {
  MigrationMode = 'migrationMode',
  MigrationType = 'migrationType',
  IsTargetDetailSet = 'isTargetDetailSet',
  IsSourceConnectionProfileSet = 'isSourceConnectionProfileSet',
  IsSourceDetailsSet = 'isSourceDetailsSet',
  IsTargetConnectionProfileSet = 'isTargetConnectionProfileSet',
  IsMigrationDetailSet = "isMigrationDetailSet",
  IsMigrationInProgress ='isMigrationInProgress',
  HasDataMigrationStarted ='hasDataMigrationStarted',
  HasSchemaMigrationStarted = 'hasSchemaMigrationStarted',
  SchemaProgressMessage = 'schemaProgressMessage',
  DataProgressMessage = 'dataProgressMessage',
  DataMigrationProgress = 'dataMigrationProgress',
  SchemaMigrationProgress = 'schemaMigrationProgress',
  HasForeignKeyUpdateStarted = 'hasForeignKeyUpdateStarted',
  ForeignKeyProgressMessage = 'foreignKeyProgressMessage',
  ForeignKeyUpdateProgress = 'foreignKeyUpdateProgress',
  GeneratingResources = 'generatingResources',
  NumberOfShards = 'numberOfShards',
  NumberOfInstances = 'numberOfInstances',
  isForeignKeySkipped = 'isForeignKeySkipped'
}

export enum TargetDetails {
  TargetDB = 'targetDb',
  Dialect = 'dialect',
  SourceConnProfile = 'sourceConnProfile',
  TargetConnProfile = 'targetConnProfile',
  ReplicationSlot = 'replicationSlot',
  Publication = 'publication'
}

export const Profile = {
  SourceProfileType : 'Source',
  TargetProfileType : 'Target',
  NewConnProfile: 'new',
  ExistingConnProfile: 'existing',
}

export const Dialect = {
  PostgreSQLDialect: 'postgresql',
  GoogleStandardSQLDialect: 'google_standard_sql'
}

export enum ProgressStatus {
	SchemaMigrationComplete = 1,
	SchemaCreationInProgress = 2,
	DataMigrationComplete = 3,
	DataWriteInProgress = 4,
	ForeignKeyUpdateInProgress = 5,
  ForeignKeyUpdateComplete = 6
}

export const DialectList = [
  { value: 'google_standard_sql', displayName: 'Google Standard SQL Dialect' },
  { value: 'postgresql', displayName: 'PostgreSQL Dialect' },
]

export const Dataflow = {
  Network: 'network',
  Subnetwork: 'subnetwork',
  HostProjectId: 'hostProjectId',
  IsDataflowConfigSet: 'isDataflowConfigSet',
}

export const ColLength = {
  StorageMaxLength: 9223372036854775807,
  StringMaxLength: 2621440,
  ByteMaxLength: 10485760,
  DataTypes: ['STRING','BYTES','VARCHAR']
}

export const DataTypes = {
  GoogleStandardSQL : ['BOOL','BYTES','DATE','FLOAT64','INT64','STRING', 'TIMESTAMP', 'NUMERIC', 'JSON'],
  PostgreSQL : ['BOOL','BYTEA','DATE','FLOAT8','INT8','VARCHAR', 'TIMESTAMPTZ', 'NUMERIC', 'JSONB']
}

export enum PersistedFormValues {
    DirectConnectForm = 'directConnectForm',
}