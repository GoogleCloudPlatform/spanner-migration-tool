export default interface IColumnTabData {
  spOrder: number
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
