export default interface IConv {
  SpSchema: Record<string, ICreateTable>
  SyntheticPKeys: Record<string, ISyntheticPKey>
  SrcSchema: Record<string, ITable>
  Issues: Record<string, number>[]
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
  Schema: string
  ColNames: string[]
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
}

export interface ISpannerForeignKey {
  Name: string
  Columns: string[]
  ReferTable: string
  ReferColumns: string[]
  OnDelete: string
  OnUpdate: string
}

// source schema
export interface ICreateTable {
  Name: string
  ColNames: string[]
  ColDefs: Record<string, IColumnDef>
  Pks: IIndexKey[]
  Fks: IForeignKey[]
  Indexes: ICreateIndex[]
  Parent: string
  Comment: string
  Id: number
  PrimaryKeyId: number
}

export interface ICreateIndex {
  Name: string
  Table: string
  Unique: boolean
  Keys: IIndexKey[]
}
export interface IForeignKey {
  Name: string
  Columns: string[]
  ReferTable: string
  ReferColumns: string[]
}

export interface IIndexKey {
  Col: string
  Desc: boolean
  Order: number
}

export interface ISrcIndexKey {
  Column: string
  Desc: boolean
  Order: number
}

export interface IColumnDef {
  Id: number
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
  Col: string
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
  TableId: number
  Columns: IPkColumnDefs[]
  PrimaryKeyId: number
}

export interface IPkColumnDefs {
  ColumnId: number
  ColumnName: string
  Desc: boolean
  Order: number
}

export interface IPrimaryKeyResponse {
  TableId: number
  Columns: {
    ColumnId: number
    ColName: string
    Desc: boolean
    Order: number
  }[]
  PrimaryKeyId: number
  Synth: boolean
}
