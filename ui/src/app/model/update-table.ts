interface IUpdateCol {
  Removed: boolean
  Rename: string
  PK: string
  NotNull: string
  ToType: string
}

export default interface IUpdateTable {
  UpdateCols: { [key: string]: IUpdateCol }
}
