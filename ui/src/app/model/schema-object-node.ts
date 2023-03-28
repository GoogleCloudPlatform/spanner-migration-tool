import { ObjectExplorerNodeType } from '../app.constants'

export default interface ISchemaObjectNode {
  name: string
  status?: string
  type: ObjectExplorerNodeType
  children?: ISchemaObjectNode[]
  pos: number
  isSpannerNode: boolean
  isDeleted?: boolean
  id: string
  parent: string
  parentId: string
}

export interface FlatNode {
  expandable: boolean
  name: string
  status: string | undefined
  type: ObjectExplorerNodeType
  pos: number
  level: number
  isSpannerNode: boolean
  isDeleted: boolean
  id: string
  parent: string
  parentId: string
}
