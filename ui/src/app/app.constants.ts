export enum InputType {
  DirectConnect = 'directConnect',
  DumpFile = 'dumpFile',
  SessionFile = 'sessionFile',
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
  DbName = 'Database Name',
  Tables = 'Tables',
  Table = 'Table Name',
  Indexes = 'Indexes',
}
