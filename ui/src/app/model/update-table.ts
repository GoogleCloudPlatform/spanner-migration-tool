interface IUpdateCol {
  Add: boolean
  Removed: boolean
  Rename: string
  NotNull: string
  ToType: string | String
  MaxColLength: string | undefined | Number
}
export interface ITableColumnChanges {
  ColumnId: string
  ColumnName: string
  Type: string | String
  UpdateColumnName: string
  UpdateType: string | String
  Size: Number
  UpdateSize: Number
}
export interface IReviewInterleaveTableChanges {
  InterleaveColumnChanges: ITableColumnChanges[]
  Table: string
}
export interface IUpdateTableArgument {
  text: string
  order: string
}

export default interface IUpdateTable {
  UpdateCols: { [key: string]: IUpdateCol }
}

export interface IReviewUpdateTable {
  Changes: IReviewInterleaveTableChanges[]
  DDL: string
}
