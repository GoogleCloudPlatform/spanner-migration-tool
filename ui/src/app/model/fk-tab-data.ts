export default interface IFkTabData {
  srcFkId: string | undefined
  spFkId: string | undefined
  spName: string
  srcName: string
  spColumns: string[]
  srcColumns: string[]
  spReferTable: string
  srcReferTable: string
  spReferColumns: string[]
  srcReferColumns: string[]
  spColIds: string[]
  spReferColumnIds: string[]
  spReferTableId: string
  srcOnDelete: string
  spOnDelete: string
  srcOnUpdate: string
  spOnUpdate: string
}
