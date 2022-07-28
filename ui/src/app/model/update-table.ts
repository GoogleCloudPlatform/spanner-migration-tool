interface IUpdateCol {
  Add: boolean
  Removed: boolean
  Rename: string
  NotNull: string
  ToType: string
}
export interface ITableColumnChanges {
  ColumnName: string
  Type: string
  UpdateColumnName: string
  UpdateType: string
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
