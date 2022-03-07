import { Injectable } from '@angular/core'
import ISchemaObjectNode from 'src/app/model/SchemaObjectNode'
import IConv from '../../model/Conv'

@Injectable({
  providedIn: 'root',
})
export class ConversionService {
  constructor() {}

  getTableNamesForSidebar(data: IConv, searceText: string): ISchemaObjectNode[] {
    let parentNode: ISchemaObjectNode = {
      name: 'Tables',
      children: Object.keys(data.SpSchema)
        .filter((name: string) => name.includes(searceText))
        .map((name: string) => {
          return { name: name, helth: 'ORANGE' }
        }),
    }
    return [parentNode]
  }
}
