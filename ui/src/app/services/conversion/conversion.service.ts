import { Injectable } from '@angular/core'
import ISchemaObjectNode from 'src/app/model/SchemaObjectNode'
import IConv from '../../model/Conv'
import IColumnTabData from '../../model/ColumnTabData'
import { ObjectExplorerNodeType } from 'src/app/app.constants'

@Injectable({
  providedIn: 'root',
})
export class ConversionService {
  constructor() {}

  createTreeNode(
    tableNames: string[],
    conversionRates: Record<string, string>
  ): ISchemaObjectNode[] {
    let parentNode: ISchemaObjectNode = {
      name: `Tables (${tableNames.length})`,
      type: ObjectExplorerNodeType.Tables,

      children: tableNames.map((name: string) => {
        return {
          name: name,
          status: conversionRates[name],
          type: ObjectExplorerNodeType.Tables,
          children: [
            {
              name: 'Indexes (8)',
              status: '',
              type: ObjectExplorerNodeType.Tables,
            },
          ],
        }
      }),
    }
    return [{ name: 'Database Name', children: [parentNode], type: ObjectExplorerNodeType.DbName }]
  }

  getColMap(tableName: string, data: IConv): IColumnTabData[] {
    if (tableName === '') {
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
    console.log('got the data.....2 in conversion', data, srcTableName, tableName)

    return data.SrcSchema[srcTableName].ColNames.map((name: string, i: number) => {
      let spColName = data.ToSpanner[srcTableName].Cols[name]
      let srcPks = data.SrcSchema[srcTableName].PrimaryKeys
      let spannerColDef = data.SpSchema[tableName].ColDefs[spColName]
      return {
        spOrder: spannerColDef ? i + 1 : '',
        srcOrder: i + 1,
        spColName: spannerColDef ? spColName : '',
        spDataType: spannerColDef ? spannerColDef.T.Name : '',
        srcColName: name,
        srcDataType: data.SrcSchema[srcTableName].ColDefs[name].Type.Name,
        spIsPk: spannerColDef
          ? data.SpSchema[tableName].Pks.map((p) => p.Col).indexOf(spColName) !== -1
          : false,
        srcIsPk: srcPks ? srcPks.map((p) => p.Column).indexOf(name) !== -1 : false,
        spIsNotNull: spannerColDef ? data.SpSchema[tableName].ColDefs[spColName].NotNull : false,
        srcIsNotNull: data.SrcSchema[srcTableName].ColDefs[name].NotNull,
      }
    })
  }
}
