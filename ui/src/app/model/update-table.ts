interface IUpdateCol {
  Add: boolean
  Removed: boolean
  Rename: string
  NotNull: string
  ToType: string
}
export interface IUpdateTableArgument {
  text: string
  order: string
}

export default interface IUpdateTable {
  UpdateCols: { [key: string]: IUpdateCol }
}

export interface IReviewUpdateTable {
  Changes: any[]
  DDL: string
}
