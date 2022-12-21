export default interface IRuleContent {
  name?: string
  type?: string
  objectType?: string
  associatedObject?: string
  enabled?: boolean
}
export interface IRule {
  name?: string
  type?: string
  objectType?: string
  associatedObjects?: string
  enabled?: boolean
  data?: any
  addedOn?: any
}
