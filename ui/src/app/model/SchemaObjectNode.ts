import { ObjectExplorerNodeType } from '../app.constants'

export default interface ISchemaObjectNode {
  name: string
  status?: string
  type: ObjectExplorerNodeType
  children?: ISchemaObjectNode[]
}
