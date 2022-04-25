import { Injectable } from '@angular/core'
import ISchemaObjectNode from 'src/app/model/SchemaObjectNode'
import IConv, { ICreateIndex, IIndexKey, Index, ISpannerForeignKey } from '../../model/Conv'
import IColumnTabData, { IIndexData } from '../../model/EditTable'
import IFkTabData from 'src/app/model/FkTabData'
import { ObjectExplorerNodeType } from 'src/app/app.constants'
import { map } from 'rxjs/operators'

@Injectable({
  providedIn: 'root',
})
export class ConversionService {
  constructor() {}

  createTreeNode(
    conv: IConv,
    conversionRates: Record<string, string>,
    searchText: string = ''
  ): ISchemaObjectNode[] {
    let spannerTableNames = Object.keys(conv.SpSchema).filter((name: string) =>
      name.toLocaleLowerCase().includes(searchText.toLocaleLowerCase())
    )
    let parentNode: ISchemaObjectNode = {
      name: `Tables (${spannerTableNames.length})`,
      type: ObjectExplorerNodeType.Tables,
      parent: '',
      pos: -1,
      isSpannerNode: true,
      children: spannerTableNames.map((name: string) => {
        let spannerTable = conv.SpSchema[name]
        return {
          name: name,
          status: conversionRates[name],
          type: ObjectExplorerNodeType.Table,
          parent: '',
          pos: -1,
          isSpannerNode: true,
          children: [
            {
              name: `Indexes (${spannerTable.Indexes ? spannerTable.Indexes.length : 0})`,
              status: '',
              type: ObjectExplorerNodeType.Indexes,
              parent: '',
              pos: -1,
              isSpannerNode: true,
              children: spannerTable.Indexes
                ? spannerTable.Indexes.map((index: ICreateIndex, i: number) => {
                    return {
                      name: index.Name,
                      type: ObjectExplorerNodeType.Index,
                      parent: name,
                      pos: i,
                      isSpannerNode: true,
                    }
                  })
                : [],
            },
          ],
        }
      }),
    }
    return [
      {
        name: 'Database Name',
        children: [parentNode],
        type: ObjectExplorerNodeType.DbName,
        parent: '',
        pos: -1,
        isSpannerNode: true,
      },
    ]
  }

  createTreeNodeForSource(
    conv: IConv,
    conversionRates: Record<string, string>,
    searchText: string = ''
  ): ISchemaObjectNode[] {
    let srcTableNames = Object.keys(conv.SrcSchema).filter((name: string) =>
      name.toLocaleLowerCase().includes(searchText.toLocaleLowerCase())
    )

    let parentNode: ISchemaObjectNode = {
      name: `Tables (${srcTableNames.length})`,
      type: ObjectExplorerNodeType.Tables,
      parent: '',
      pos: -1,
      isSpannerNode: false,
      children: srcTableNames.map((name: string) => {
        let srcTable = conv.SrcSchema[name]
        let spname = conv.ToSpanner[name].Name
        return {
          name: name,
          status: conversionRates[spname],
          type: ObjectExplorerNodeType.Table,
          parent: '',
          pos: -1,
          isSpannerNode: false,
          children: [
            {
              name: `Indexes (${srcTable.Indexes?.length || '0'})`,
              status: '',
              type: ObjectExplorerNodeType.Indexes,
              parent: '',
              pos: -1,
              isSpannerNode: false,
              children: srcTable.Indexes
                ? srcTable.Indexes.map((index: Index, i: number) => {
                    return {
                      name: index.Name,
                      type: ObjectExplorerNodeType.Index,
                      parent: name,
                      isSpannerNode: false,
                      pos: i,
                    }
                  })
                : [],
            },
          ],
        }
      }),
    }

    return [
      {
        name: 'Database Name',
        children: [parentNode],
        type: ObjectExplorerNodeType.DbName,
        parent: '',
        isSpannerNode: false,
        pos: -1,
      },
    ]
  }
  getColumnMapping(tableName: string, data: IConv): IColumnTabData[] {
    let srcTableName = data.ToSource[tableName].Name
    // console.log('got the data.....2 in conversion', data, srcTableName, tableName)

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

  getFkMapping(tableName: string, data: IConv): IFkTabData[] {
    let srcTableName: string = data.ToSource[tableName].Name
    let spFks = data.SpSchema[tableName].Fks
    if (!spFks) {
      return []
    }
    let spFklength: number = spFks.length
    return data.SrcSchema[srcTableName].ForeignKeys.map((item: ISpannerForeignKey, i: number) => {
      spFklength = spFklength - 1
      if (
        data.SpSchema[tableName].Fks.length != data.SrcSchema[srcTableName].ForeignKeys.length &&
        spFklength < 0
      ) {
        return {
          spName: '',
          srcName: data.SrcSchema[srcTableName].ForeignKeys[i].Name,
          spColumns: [],
          srcColumns: data.SrcSchema[srcTableName].ForeignKeys[i].Columns,
          spReferTable: '',
          srcReferTable: data.SrcSchema[srcTableName].ForeignKeys[i].ReferTable,
          spReferColumns: [],
          srcReferColumns: data.SrcSchema[srcTableName].ForeignKeys[i].ReferColumns,
        }
      } else {
        return {
          spName: data.SpSchema[tableName].Fks[i].Name,
          srcName: data.SrcSchema[srcTableName].ForeignKeys[i].Name,
          spColumns: data.SpSchema[tableName].Fks[i].Columns,
          srcColumns: data.SrcSchema[srcTableName].ForeignKeys[i].Columns,
          spReferTable: data.SpSchema[tableName].Fks[i].ReferTable,
          srcReferTable: data.SrcSchema[srcTableName].ForeignKeys[i].ReferTable,
          spReferColumns: data.SpSchema[tableName].Fks[i].ReferColumns,
          srcReferColumns: data.SrcSchema[srcTableName].ForeignKeys[i].ReferColumns,
        }
      }
    })
  }

  getIndexMapping(tableName: string, data: IConv, indexName: string): IIndexData[] {
    let srcTableName = data.ToSource[tableName].Name
    let spIndex = data.SpSchema[tableName].Indexes.filter((idx) => idx.Name === indexName)[0]
    let srcIndexs = data.SrcSchema[srcTableName].Indexes.filter((idx) => idx.Name === indexName)

    let res: IIndexData[] = spIndex.Keys.map((idx: IIndexKey, i: number) => {
      return {
        srcColName: srcIndexs.length > 0 ? srcIndexs[0].Keys[i].Column : '',
        srcOrder: srcIndexs.length > 0 ? i + 1 : '',
        spColName: idx.Col,
        spOrder: i + 1,
      }
    })
    console.log(res)

    return res
  }
}
