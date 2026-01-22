import {IDefaultValue, IGeneratedColumn} from "./conv"

export default interface IColumnTabData {
  spOrder: number | string
  srcOrder: number | string
  srcColName: string
  srcDataType: string
  spColName: string
  spDataType: string | String
  spAutoGen: AutoGen
  spSkipRangeMin: string
  spSkipRangeMax: string
  spStartCounterWith: string
  spIsPk: boolean
  srcIsPk: boolean
  spIsNotNull: boolean
  srcIsNotNull: boolean
  srcDefaultValue: string
  srcGeneratedColExp: string
  srcGeneratedColExpType: string
  srcId: string
  spId: string
  srcColMaxLength: Number | string | undefined
  spColMaxLength: Number | string | undefined
  spCassandraOption: string
  srcAutoGen: AutoGen
  spDefaultValue: IDefaultValue
  spGeneratedColumn: IGeneratedColumn
  spGeneratedColumnType: string
}

export interface AutoGen {
  Name: string;
  GenerationType: string;
  IdentityOptions: IdentityOptions;
}

export interface IdentityOptions {
  SkipRangeMin: string;
  SkipRangeMax: string;
  StartCounterWith: string;
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
}

export interface IColMaxLength {
  spDataType: string,
  spColMaxLength: Number | string | undefined
}

export interface IAddColumnProps {
  dialect: string
  tableId: string
}
