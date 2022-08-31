import { Injectable } from '@angular/core'
import ISchemaObjectNode from 'src/app/model/schema-object-node'
import IConv, {
  ICreateIndex,
  IIndexKey,
  IIndex,
  ISpannerForeignKey,
  IColumnDef,
} from '../../model/conv'
import IColumnTabData, { IIndexData } from '../../model/edit-table'
import IFkTabData from 'src/app/model/fk-tab-data'
import { ObjectExplorerNodeType } from 'src/app/app.constants'

@Injectable({
  providedIn: 'root',
})
export class ConversionService {
  constructor() {}

  createTreeNode(
    conv: IConv,
    conversionRates: Record<string, string>,
    searchText: string = '',
    sortOrder: string = ''
  ): ISchemaObjectNode[] {
    let spannerTableNames = Object.keys(conv.SpSchema).filter((name: string) =>
      name.toLocaleLowerCase().includes(searchText.toLocaleLowerCase())
    )
    let srcTableNames = Object.keys(conv.SrcSchema).filter((name: string) =>
      name.toLocaleLowerCase().replace('.', '_').includes(searchText.toLocaleLowerCase())
    )

    let deletedTableNames = srcTableNames.filter((srcTableName: string) => {
      if (spannerTableNames.indexOf(srcTableName) > -1) {
        return false
      }
      return true
    })
    let parentNode: ISchemaObjectNode = {
      name: `Tables (${srcTableNames.length})`,
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
              parent: name,
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
    deletedTableNames.forEach((tableName: string) => {
      parentNode.children?.push({
        name: tableName,
        status: 'DARK',
        type: ObjectExplorerNodeType.Table,
        parent: '',
        pos: -1,
        isSpannerNode: true,
        children: [],
        isDeleted: true,
      })
    })
    if (sortOrder === 'asc') {
      parentNode.children?.sort((a, b) => (a.name > b.name ? 1 : b.name > a.name ? -1 : 0))
    } else if (sortOrder === 'desc') {
      parentNode.children?.sort((a, b) => (b.name > a.name ? 1 : a.name > b.name ? -1 : 0))
    }

    return [
      {
        name: conv.DatabaseName,
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
    searchText: string = '',
    sortOrder: string = ''
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
        let spname = conv.ToSpanner[name] ? conv.ToSpanner[name].Name : ''
        return {
          name: name,
          status: conversionRates[spname] ? conversionRates[spname] : 'NONE',
          type: ObjectExplorerNodeType.Table,
          parent: '',
          pos: -1,
          isSpannerNode: false,
          children: [
            {
              name: `Indexes (${srcTable.Indexes?.length || '0'})`,
              status: '',
              type: ObjectExplorerNodeType.Indexes,
              parent: name,
              pos: -1,
              isSpannerNode: false,
              children: srcTable.Indexes
                ? srcTable.Indexes.map((index: IIndex, i: number) => {
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
    if (sortOrder === 'asc') {
      parentNode.children?.sort((a, b) => (a.name > b.name ? 1 : b.name > a.name ? -1 : 0))
    } else if (sortOrder === 'desc') {
      parentNode.children?.sort((a, b) => (b.name > a.name ? 1 : a.name > b.name ? -1 : 0))
    }

    return [
      {
        name: conv.DatabaseName,
        children: [parentNode],
        type: ObjectExplorerNodeType.DbName,
        parent: '',
        isSpannerNode: false,
        pos: -1,
      },
    ]
  }

  getColumnMapping(tableName: string, data: IConv): IColumnTabData[] {
    let srcTableName = tableName

    return data.SrcSchema[srcTableName].ColNames.map((name: string, i: number) => {
      let colId = data.SrcSchema[srcTableName].ColDefs[name].Id
      let srcPks = data.SrcSchema[srcTableName].PrimaryKeys
      let spannerColDef = this.getSpannerColDefFromId(tableName, colId, data)
      let spColName = spannerColDef ? spannerColDef.Name : ''
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

  getPkMapping(tableData: IColumnTabData[]): IColumnTabData[] {
    let pkColumns = tableData.filter((column: IColumnTabData) => {
      return column.spIsPk || column.srcIsPk
    })
    return JSON.parse(JSON.stringify(pkColumns))
  }

  getFkMapping(tableName: string, data: IConv): IFkTabData[] {
    //Todo : Need to differentiate between src and spanner table name in argument
    // let srcTableName: string = data.ToSource[tableName].Name
    let srcTableName = tableName
    let spFks =
      data.SpSchema[tableName] && data.SpSchema[tableName].Fks ? data.SpSchema[tableName].Fks : []
    let srcFks = data.SrcSchema[srcTableName]?.ForeignKeys

    if (!srcFks) {
      return []
    }
    let spFklength: number = spFks.length
    return data.SrcSchema[srcTableName].ForeignKeys.map((item: ISpannerForeignKey, i: number) => {
      spFklength = spFklength - 1
      if (
        data.SpSchema[tableName] &&
        data.SpSchema[tableName].Fks?.length != data.SrcSchema[srcTableName].ForeignKeys.length &&
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
          spName: data.SpSchema[tableName] ? data.SpSchema[tableName].Fks[i].Name : '',
          srcName: data.SrcSchema[srcTableName].ForeignKeys[i].Name,
          spColumns: data.SpSchema[tableName] ? data.SpSchema[tableName].Fks[i].Columns : [],
          srcColumns: data.SrcSchema[srcTableName].ForeignKeys[i].Columns,
          spReferTable: data.SpSchema[tableName] ? data.SpSchema[tableName].Fks[i].ReferTable : '',
          srcReferTable: data.SrcSchema[srcTableName].ForeignKeys[i].ReferTable,
          spReferColumns: data.SpSchema[tableName]
            ? data.SpSchema[tableName].Fks[i].ReferColumns
            : [],
          srcReferColumns: data.SrcSchema[srcTableName].ForeignKeys[i].ReferColumns,
        }
      }
    })
  }

  getIndexMapping(tableName: string, data: IConv, indexName: string): IIndexData[] {
    // let srcTableName = data.ToSource[tableName].Name
    let srcTableName = tableName
    let spIndex = data.SpSchema[tableName]?.Indexes.filter((idx) => idx.Name === indexName)[0]
    let srcIndexs = data.SrcSchema[srcTableName].Indexes?.filter((idx) => idx.Name === indexName)

    let res: IIndexData[] = spIndex
      ? spIndex.Keys.map((idx: IIndexKey, i: number) => {
          return {
            srcColName:
              srcIndexs && srcIndexs.length > 0 && srcIndexs[0].Keys.length > i
                ? srcIndexs[0].Keys[i].Column
                : '',
            srcOrder:
              srcIndexs && srcIndexs.length > 0 && srcIndexs[0].Keys.length > i ? i + 1 : '',
            srcDesc:
              srcIndexs && srcIndexs.length > 0 && srcIndexs[0].Keys.length > i
                ? srcIndexs[0].Keys[i].Desc
                : undefined,
            spColName: idx.Col,
            spOrder: i + 1,
            spDesc: idx.Desc,
          }
        })
      : []
    let spKeyLength = spIndex ? spIndex.Keys.length : 0
    if (srcIndexs && srcIndexs[0] && spKeyLength < srcIndexs[0].Keys.length) {
      srcIndexs[0].Keys.forEach((idx, index) => {
        if (index >= spKeyLength) {
          res.push({
            srcColName: idx.Column,
            srcOrder: index + 1,
            srcDesc: idx.Desc,
            spColName: undefined,
            spOrder: undefined,
            spDesc: undefined,
          })
        }
      })
    }
    return res
  }
  getSpannerColDefFromId(tableName: string, id: string, data: IConv): IColumnDef | null {
    let res: IColumnDef | null = null
    Object.keys(data.SpSchema[tableName].ColDefs).forEach((colName) => {
      if (data.SpSchema[tableName].ColDefs[colName].Id == id) {
        res = data.SpSchema[tableName].ColDefs[colName]
      }
    })
    return res
  }
}
