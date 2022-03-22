import { Injectable } from '@angular/core'
import ISchemaObjectNode from 'src/app/model/SchemaObjectNode'
import IConv from '../../model/Conv'

interface IColMap {
  srcColName: string
  srcDataType: string
  spColName: string
  spDataType: string
}
@Injectable({
  providedIn: 'root',
})
export class ConversionService {
  constructor() {}

  getTableNamesForSidebar(
    data: IConv,
    conversionRate: Record<string, string>
  ): ISchemaObjectNode[] {
    let parentNode: ISchemaObjectNode = {
      name: 'Tables',
      children: Object.keys(data.SpSchema).map((name: string) => {
        console.log(name)

        return { name: name, helth: conversionRate[name] }
      }),
    }
    return [parentNode]
  }

  createTreeNode(
    tableNames: string[],
    conversionRates: Record<string, string>
  ): ISchemaObjectNode[] {
    let parentNode: ISchemaObjectNode = {
      name: 'Tables',
      children: tableNames.map((name: string) => {
        return { name: name, helth: conversionRates[name] }
      }),
    }
    return [{ name: 'Database Name', children: [parentNode] }]
  }

  getColMap(tableName: string, data: IConv): IColMap[] {
    if (tableName === ""){
      return [{
        spColName: "",
        spDataType: "",
        srcColName: "",
        srcDataType: "",
      }]
    }
    let srcTableName = data.ToSource[tableName].Name
    return data.SrcSchema[srcTableName].ColNames.map((name: string, i: number) => {
      let spColName = data.SpSchema[tableName].ColNames[i]
      return {
        spColName: spColName,
        spDataType: data.SpSchema[tableName].ColDefs[spColName].T.Name,
        srcColName: name,
        srcDataType: data.SrcSchema[srcTableName].ColDefs[name].Type.Name,
      }
    })
  }
}
