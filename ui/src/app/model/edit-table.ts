export default interface IColumnTabData {
  spOrder: number | string
  srcOrder: number
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
  spColName: string | undefined
  srcDesc: boolean | undefined
  srcOrder: number | string
  spOrder: number | string | undefined
  spDesc: boolean | undefined
}
