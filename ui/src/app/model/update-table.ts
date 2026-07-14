import {IDefaultValue, IGeneratedColumn} from "./conv"
import { AutoGen } from "./edit-table"

interface IUpdateCol {
  Add: boolean
  Removed: boolean
  Rename: string
  NotNull: string
  ToType: string | String
  MaxColLength: string | undefined | Number
  AutoGen: AutoGen
  DefaultValue: IDefaultValue
  GeneratedColumn: IGeneratedColumn
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
export interface IUpdateTableArgument {
  text: string
  order: string
}

export default interface IUpdateTable {
  UpdateCols: { [key: string]: IUpdateCol }
}

export interface IReviewUpdateTable {
  DDL: string
}

export interface IAddColumn {
  Name: string
  Datatype: string
  Length: number
  IsNullable: boolean
  AutoGen: AutoGen
  Option?: string
}
