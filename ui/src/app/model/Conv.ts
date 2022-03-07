export default interface IConv {
  mode: number
  SpSchema: Record<string, ICreateTable>
  SyntheticPKeys: Record<string, ISyntheticPKey>
  SrcSchema: Record<string, Table>
  Issues: Record<string, number>[]
  ToSpanner: Record<string, NameAndCols>
  ToSource: Record<string, NameAndCols>
  UsedNames: Record<string, boolean>
  TimezoneOffset: string
  Stats: IStats
  UniquePKey: Record<string, string[]>
}

export interface IStats {
  Rows: Record<string, number> // Count of rows encountered during processing (a + b + c + d), broken down by source table.
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
export interface Table {
  Name: string
  Schema: string
  ColNames: string[]
  ColDefs: Record<string, IColumn>
  PrimaryKeys: IIndexKey[]
  ForeignKeys: ISpannerForeignKey[]
  Indexes: Index[]
}

export interface IColumn {
  Name: string
  Type: ISpennerType
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

export interface ISpennerType {
  Name: string
  Mods: number[]
  ArrayBounds: number[]
}

export interface Index {
  Name: string
  Unique: boolean
  Keys: IIndexKey[]
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
}

export interface IColumnDef {
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
