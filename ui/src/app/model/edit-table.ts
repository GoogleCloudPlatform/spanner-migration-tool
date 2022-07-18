export default interface IColumnTabData {
  spOrder: number | string
  srcOrder: number | string
  srcColName: string
  srcDataType: string
  spColName: string
  spDataType: string
  spIsPk: boolean
  srcIsPk: boolean
  spIsNotNull: boolean
  srcIsNotNull: boolean
}

export interface IIndexData {
  srcColName: string
  spColName: string
  srcDesc: boolean | undefined
  srcOrder: number | string
  spOrder: number | string
  spDesc: boolean
}
