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
    let spannerTableIds = spannerTableNames.map((name: string) => conv.SpSchema[name].Id)
    let srcTableNames = Object.keys(conv.SrcSchema)

    let deletedTableNames = srcTableNames.filter((srcTableName: string) => {
      if (spannerTableIds.indexOf(conv.SrcSchema[srcTableName].Id) > -1) {
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
      id: '',
      parentId: '',
      children: spannerTableNames.map((name: string) => {
        let spannerTable = conv.SpSchema[name]
        return {
          name: name,
          status: conversionRates[name],
          type: ObjectExplorerNodeType.Table,
          parent: '',
          pos: -1,
          isSpannerNode: true,
          id: spannerTable.Id,
          parentId: '',
          children: [
            {
              name: `Indexes (${spannerTable.Indexes ? spannerTable.Indexes.length : 0})`,
              status: '',
              type: ObjectExplorerNodeType.Indexes,
              parent: name,
              pos: -1,
              isSpannerNode: true,
              id: '',
              parentId: spannerTable.Id,
              children: spannerTable.Indexes
                ? spannerTable.Indexes.map((index: ICreateIndex, i: number) => {
                    return {
                      name: index.Name,
                      type: ObjectExplorerNodeType.Index,
                      parent: name,
                      pos: i,
                      isSpannerNode: true,
                      id: index.Id,
                      parentId: spannerTable.Id,
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

    deletedTableNames.forEach((tableName: string) => {
      parentNode.children?.push({
        name: tableName.replace(/[^A-Za-z0-9_]/g, '_'),
        status: 'DARK',
        type: ObjectExplorerNodeType.Table,
        parent: '',
        pos: -1,
        isSpannerNode: true,
        children: [],
        isDeleted: true,
        id: conv.SrcSchema[tableName].Id,
        parentId: '',
      })
    })
    return [
      {
        name: conv.DatabaseName,
        children: [parentNode],
        type: ObjectExplorerNodeType.DbName,
        parent: '',
        pos: -1,
        isSpannerNode: true,
        id: '',
        parentId: '',
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
      id: '',
      parentId: '',
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
          id: srcTable.Id,
          parentId: '',
          children: [
            {
              name: `Indexes (${srcTable.Indexes?.length || '0'})`,
              status: '',
              type: ObjectExplorerNodeType.Indexes,
              parent: name,
              pos: -1,
              isSpannerNode: false,
              id: '',
              parentId: srcTable.Id,
              children: srcTable.Indexes
                ? srcTable.Indexes.map((index: IIndex, i: number) => {
                    return {
                      name: index.Name,
                      type: ObjectExplorerNodeType.Index,
                      parent: name,
                      isSpannerNode: false,
                      pos: i,
                      id: index.Id,
                      parentId: srcTable.Id,
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
        id: '',
        parentId: '',
      },
    ]
  }

  getColumnMapping(id: string, data: IConv): IColumnTabData[] {
    let srcTableName = this.getSourceTableNameFromId(id, data)
    let spTableName = this.getSpannerTableNameFromId(id, data)

    let srcTableIds = Object.keys(data.SrcSchema[srcTableName].ColDefs).map(
      (name: string) => data.SrcSchema[srcTableName].ColDefs[name].Id
    )

    const res: IColumnTabData[] = data.SrcSchema[srcTableName].ColNames.map(
      (name: string, i: number) => {
        let spColName = data.ToSpanner[srcTableName]?.Cols[name]
        let srcPks = data.SrcSchema[srcTableName].PrimaryKeys
        let spPkOrder
        if (spTableName) {
          data.SpSchema[spTableName].Pks.forEach((col: IIndexKey) => {
            if (col.Col == name) {
              spPkOrder = col.Order
            }
          })
        }
        let spannerColDef = spTableName ? data.SpSchema[spTableName]?.ColDefs[spColName] : null
        return {
          spOrder: spannerColDef ? i + 1 : '',
          srcOrder: i + 1,
          spColName: spannerColDef ? spColName : '',
          spDataType: spannerColDef ? spannerColDef.T.Name : '',
          srcColName: name,
          srcDataType: data.SrcSchema[srcTableName].ColDefs[name].Type.Name,
          spIsPk:
            spannerColDef && spTableName
              ? data.SpSchema[spTableName].Pks?.map((p) => p.Col).indexOf(spColName) !== -1
              : false,
          srcIsPk: srcPks ? srcPks.map((p) => p.Column).indexOf(name) !== -1 : false,
          spIsNotNull:
            spannerColDef && spTableName
              ? data.SpSchema[spTableName].ColDefs[spColName].NotNull
              : false,
          srcIsNotNull: data.SrcSchema[srcTableName].ColDefs[name].NotNull,
        }
      }
    )
    if (spTableName) {
      data.SpSchema[spTableName]?.ColNames.forEach((name: string, i: number) => {
        if (spTableName && srcTableIds.indexOf(data.SpSchema[spTableName].ColDefs[name].Id) < 0) {
          let spannerColDef = spTableName ? data.SpSchema[spTableName].ColDefs[name] : null
          res.push({
            spOrder: i + 1,
            srcOrder: '',
            spColName: name,
            spDataType: spannerColDef ? spannerColDef.T.Name : '',
            srcColName: '',
            srcDataType: '',
            spIsPk:
              spannerColDef && spTableName
                ? data.SpSchema[spTableName].Pks.map((p) => p.Col).indexOf(name) !== -1
                : false,
            srcIsPk: false,
            spIsNotNull:
              spannerColDef && spTableName
                ? data.SpSchema[spTableName].ColDefs[name].NotNull
                : false,
            srcIsNotNull: false,
          })
        }
      })
    }
    return res
  }

  getPkMapping(tableData: IColumnTabData[]): IColumnTabData[] {
    let pkColumns = tableData.filter((column: IColumnTabData) => {
      return column.spIsPk || column.srcIsPk
    })
    return JSON.parse(JSON.stringify(pkColumns))
  }

  getFkMapping(id: string, data: IConv): IFkTabData[] {
    let srcTableName = this.getSourceTableNameFromId(id, data)
    let spTableName = this.getSpannerTableNameFromId(id, data)

    let spFks =
      spTableName && data.SpSchema[spTableName] && data.SpSchema[spTableName].Fks
        ? data.SpSchema[spTableName].Fks
        : []
    let srcFks = data.SrcSchema[srcTableName]?.ForeignKeys

    if (!srcFks) {
      return []
    }
    let spFklength: number = spFks.length
    return data.SrcSchema[srcTableName].ForeignKeys.map((item: ISpannerForeignKey, i: number) => {
      spFklength = spFklength - 1
      if (
        spTableName &&
        data.SpSchema[spTableName] &&
        data.SpSchema[spTableName].Fks?.length != data.SrcSchema[srcTableName].ForeignKeys.length &&
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
          spName:
            spTableName && data.SpSchema[spTableName] ? data.SpSchema[spTableName].Fks[i].Name : '',
          srcName: data.SrcSchema[srcTableName].ForeignKeys[i].Name,
          spColumns:
            spTableName && data.SpSchema[spTableName]
              ? data.SpSchema[spTableName].Fks[i].Columns
              : [],
          srcColumns: data.SrcSchema[srcTableName].ForeignKeys[i].Columns,
          spReferTable:
            spTableName && data.SpSchema[spTableName]
              ? data.SpSchema[spTableName].Fks[i].ReferTable
              : '',
          srcReferTable: data.SrcSchema[srcTableName].ForeignKeys[i].ReferTable,
          spReferColumns:
            spTableName && data.SpSchema[spTableName]
              ? data.SpSchema[spTableName].Fks[i].ReferColumns
              : [],
          srcReferColumns: data.SrcSchema[srcTableName].ForeignKeys[i].ReferColumns,
        }
      }
    })
  }

  getIndexMapping(tableId: string, data: IConv, indexId: string): IIndexData[] {
    let srcTableName = this.getSourceTableNameFromId(tableId, data)
    let spTableName = this.getSpannerTableNameFromId(tableId, data)
    let spIndex = spTableName
      ? data.SpSchema[spTableName]?.Indexes.filter((idx) => idx.Id === indexId)[0]
      : null
    let srcIndexs = data.SrcSchema[srcTableName].Indexes?.filter((idx) => idx.Id === indexId)

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
            spOrder: idx.Order,
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
            spColName: '',
            spOrder: '',
            spDesc: '',
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

  getSourceTableNameFromId(id: string, conv: IConv): string {
    let srcName: string = ''
    Object.keys(conv.SrcSchema).forEach((key: string) => {
      if (conv.SrcSchema[key].Id === id) {
        srcName = conv.SrcSchema[key].Name
      }
    })
    return srcName
  }
  getSpannerTableNameFromId(id: string, conv: IConv): string | null {
    let spName: string | null = null
    Object.keys(conv.SpSchema).forEach((key: string) => {
      if (conv.SpSchema[key].Id === id) {
        spName = conv.SpSchema[key].Name
      }
    })
    return spName
  }
}
