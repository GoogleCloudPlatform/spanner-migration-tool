export default interface IRule {
  Name?: string
  Type?: string
  ObjectType?: string
  AssociatedObjects?: string
  Enabled?: boolean
  Data?: any
  AddedOn?: any
  Id?: string
}

export interface ITransformation {
  Name?: string
  Type?: string
  ObjectType?: string
  AssociatedObjects?: string
  Enabled?: boolean
  AddedOn?: any
  Id?: string
  Function?: string
  Input?: any
  Action?: string
  ActionConfig?:any
}