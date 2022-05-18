import { ObjectExplorerNodeType } from '../app.constants'

export default interface ISchemaObjectNode {
  name: string
  status?: string
  type: ObjectExplorerNodeType
  children?: ISchemaObjectNode[]
  parent: string
  pos: number
  isSpannerNode: boolean
}

export interface FlatNode {
  expandable: boolean
  name: string
  status: string | undefined
  type: ObjectExplorerNodeType
  parent: string
  pos: number
  level: number
  isSpannerNode: boolean
}
