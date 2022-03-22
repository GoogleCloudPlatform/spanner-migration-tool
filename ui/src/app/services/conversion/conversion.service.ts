import { Injectable } from '@angular/core'
import ISchemaObjectNode from 'src/app/model/SchemaObjectNode'
import IConv from '../../model/Conv'
import IColumnTabData from '../../model/ColumnTabData'

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

  getColMap(tableName: string, data: IConv): IColumnTabData[] {
    if (tableName === ""){
      return [
        {
          spOrder: 0,
          srcOrder: 0,
          spColName: '',
          spDataType: '',
          srcColName: '',
          srcDataType: '',
          spIsPk: false,
          srcIsPk: false,
          spIsNotNull: false,
          srcIsNotNull: false,
        },
      ]
    }
    let srcTableName = data.ToSource[tableName].Name
    return data.SrcSchema[srcTableName].ColNames.map((name: string, i: number) => {
      let spColName = data.SpSchema[tableName].ColNames[i]
      return {
        spOrder: i + 1,
        srcOrder: i + 1,
        spColName: spColName,
        spDataType: data.SpSchema[tableName].ColDefs[spColName].T.Name,
        srcColName: name,
        srcDataType: data.SrcSchema[srcTableName].ColDefs[name].Type.Name,
        spIsPk: data.SpSchema[tableName].Pks.map((p) => p.Col).indexOf(spColName) != -1,
        srcIsPk: data.SrcSchema[srcTableName].PrimaryKeys.map((p) => p.Column).indexOf(name) != -1,
        spIsNotNull: data.SpSchema[tableName].ColDefs[spColName].NotNull,
        srcIsNotNull: data.SrcSchema[srcTableName].ColDefs[name].NotNull,
      }
    })
  }
}
