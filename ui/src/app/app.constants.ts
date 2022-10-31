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
  SchemaMigrationProgress = 'schemaMigrationProgress'
}

export enum TargetDetails {
  TargetDB = 'targetDb',
  Dialect = 'dialect',
  SourceConnProfile = 'sourceConnProfile',
  TargetConnProfile = 'targetConnProfile'
}

export const Profile = {
  SourceProfileType : 'Source',
  TargetProfileType : 'Target',
  NewConnProfile: 'new',
  ExistingConnProfile: 'existing',
}

export enum ProgressStatus {
	SchemaMigrationComplete = 1,
	SchemaCreationInProgress = 2,
	DataMigrationComplete = 3,
	DataWriteInProgress = 4,
	ForeignKeyUpdateInProgress = 5
}
