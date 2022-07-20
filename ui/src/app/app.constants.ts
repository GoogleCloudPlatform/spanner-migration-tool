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
