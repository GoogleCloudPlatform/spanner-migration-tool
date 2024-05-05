export default interface IColumnTabData {
  spOrder: number | string
  srcOrder: number | string
  srcColName: string
  srcDataType: string
  spColName: string
  spDataType: string | String
  spAutoGen: AutoGen
  spIsPk: boolean
  srcIsPk: boolean
  spIsNotNull: boolean
  srcIsNotNull: boolean
  srcId: string
  spId: string
  srcColMaxLength: Number | string | undefined
  spColMaxLength: Number | string | undefined
}

export interface AutoGen {
  Name: string;
  GenerationType: string;
}

export interface IIndexData {
  srcColId: string | undefined
  spColId: string | undefined
  srcColName: string
  spColName: string | undefined
  srcDesc: boolean | undefined | string
  srcOrder: number | string
  spOrder: number | string | undefined
  spDesc: boolean | undefined | string
}

export interface ISequenceData {
  spSeqName?: string | undefined
  spSequenceKind?: string | undefined
  spSkipRangeMin?: string | undefined
  spSkipRangeMax?: string | undefined
  spStartWithCounter?: string | undefined
  srcName?: string | undefined
  srcSequenceKind?: string | undefined
  srcSkipRangeMin?: string | undefined
  srcSkipRangeMax?: string | undefined
  srcStartWithCounter?: string | undefined
}

export interface IColMaxLength {
  spDataType: string,
  spColMaxLength: Number | string | undefined
}

export interface IAddColumnProps {
  dialect: string
  tableId: string
}