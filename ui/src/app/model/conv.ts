import IRule from './rule'

export default interface IConv {
  SpSchema: Record<string, ICreateTable>
  SyntheticPKeys: Record<string, ISyntheticPKey>
  SrcSchema: Record<string, ITable>
  SchemaIssues: Record<string, number>[]
  Rules: IRule[]
  ToSpanner: Record<string, NameAndCols>
  ToSource: Record<string, NameAndCols>
  UsedNames: Record<string, boolean>
  TimezoneOffset: string
  Stats: IStats
  UniquePKey: Record<string, string[]>
  SessionName: string
  DatabaseType: string
  DatabaseName: string
  EditorName: string
  SpDialect: string
}

export interface IFkeyAndIdxs {
  Name: string
  ForeignKey: Record<string, string>
  Index: Record<string, string>
}

export interface IStats {
  Rows: Record<string, number>
  GoodRows: Record<string, number>
  BadRows: Record<string, number>
  Unexpected: Record<string, number> // Count of unexpected conditions, broken down by condition description.
  Reparsed: number
}

export interface NameAndCols {
  Name: string
  Cols: Record<string, string>
}

// Spanner schema
export interface ITable {
  Name: string
  Id: string
  Schema: string
  ColIds: string[]
  ColDefs: Record<string, IColumn>
  PrimaryKeys: ISrcIndexKey[]
  ForeignKeys: ISpannerForeignKey[]
  Indexes: IIndex[]
}

export interface IColumn {
  Name: string
  Type: ISpannerType
  NotNull: boolean
  Ignored: IIgnored
  Id: string
}

export interface IIgnored {
  Check: boolean
  Identity: boolean
  Default: boolean
  Exclusion: boolean
  ForeignKey: boolean
  AutoIncrement: boolean
}

export interface ISpannerType {
  Name: string
  Mods: number[]
  ArrayBounds: number[]
}

export interface IIndex {
  Name: string
  Unique: boolean
  Keys: ISrcIndexKey[]
  Id: string
}

export interface ISpannerForeignKey {
  Name: string
  ColIds: string[]
  ReferTableId: string
  ReferColumnIds: string[]
  OnDelete: string
  OnUpdate: string
  Id: string
}

// source schema
export interface ICreateTable {
  Name: string
  ColIds: string[]
  ColDefs: Record<string, IColumnDef>
  PrimaryKeys: IIndexKey[]
  ForeignKeys: IForeignKey[]
  Indexes: ICreateIndex[]
  ParentId: string
  Comment: string
  Id: string
}

export interface ICreateIndex {
  Name: string
  TableId: string
  Unique: boolean
  Keys: IIndexKey[]
  Id: string
}

export interface IForeignKey {
  Name: string
  ColIds: string[]
  ReferTableId: string
  ReferColumnIds: string[]
  Id: string | undefined
}

export interface IIndexKey {
  ColId: string
  Desc: boolean
  Order: number
}

export interface ISrcIndexKey {
  ColId: string
  Desc: boolean
  Order: number
}

export interface IColumnDef {
  Id: string
  Name: string
  T: IType
  NotNull: boolean
  Comment: string
}

export interface IType {
  Name: string
  Len: Number
  IsArray: boolean
}

export interface ISyntheticPKey {
  ColId: string
  Sequence: Number
}
export interface ITableInterleaveStatus {
  Possible: boolean
  Parent: string
  Comment: string
}

export interface IInterleaveStatus {
  TableInterleaveStatus: ITableInterleaveStatus
}

export interface IPrimaryKey {
  TableId: string
  Columns: IPkColumnDefs[]
}

export interface IPkColumnDefs {
  ColumnId: string
  Desc: boolean
  Order: number
}

export interface ISessionSummary {
  DatabaseType: string
  ConnectionDetail: string
  SourceTableCount: number
  SpannerTableCount: number
  SourceIndexCount: number
  SpannerIndexCount: number
  ConnectionType: string
  SourceDatabaseName: string
  Region: string
  NodeCount: number
  ProcessingUnits: number
  Instance: string
  Dialect: string
}

export interface ISpannerDetails {
  Region: string
  Instance: string
  Dialect: string
}

export interface ITableIdAndName {
  Id: string
  Name: string
}