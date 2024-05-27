import { Injectable } from '@angular/core'
import ISchemaObjectNode from 'src/app/model/schema-object-node'
import IConv, {
  ICreateIndex,
  IIndexKey,
  IIndex,
  ISpannerForeignKey,
  IForeignKey,
  ISrcIndexKey,
  IColumnDef,
} from '../../model/conv'
import IColumnTabData, { IIndexData, ISequenceData } from '../../model/edit-table'
import IFkTabData from 'src/app/model/fk-tab-data'
import { ColLength, Dialect, ObjectExplorerNodeType, StorageKeys, autoGenSupportedDbs } from 'src/app/app.constants'
import { BehaviorSubject } from 'rxjs'
import { FetchService } from '../fetch/fetch.service'
import { extractSourceDbName } from 'src/app/utils/utils'

@Injectable({
  providedIn: 'root',
})
export class ConversionService {
  constructor(private fetch: FetchService,) { }
  private standardTypeToPGSQLTypeMapSub = new BehaviorSubject(new Map<string, string>())
  private pgSQLToStandardTypeTypeMapSub = new BehaviorSubject(new Map<string, string>())

  standardTypeToPGSQLTypeMap = this.standardTypeToPGSQLTypeMapSub.asObservable()
  pgSQLToStandardTypeTypeMap = this.pgSQLToStandardTypeTypeMapSub.asObservable()
  srcDbName: string = localStorage.getItem(StorageKeys.SourceDbName) as string

  getStandardTypeToPGSQLTypemap() {
    return this.fetch.getStandardTypeToPGSQLTypemap().subscribe({
      next: (standardTypeToPGSQLTypeMap: any) => {
        this.standardTypeToPGSQLTypeMapSub.next(new Map<string, string>(Object.entries(standardTypeToPGSQLTypeMap)))
      },
    })
  }

  getPGSQLToStandardTypeTypemap() {
    return this.fetch.getPGSQLToStandardTypeTypemap().subscribe({
      next: (pgSQLToStandardTypeTypeMap: any) => {
        this.pgSQLToStandardTypeTypeMapSub.next(new Map<string, string>(Object.entries(pgSQLToStandardTypeTypeMap)))
      },
    })
  }

  createTreeNode(
    conv: IConv,
    conversionRates: Record<string, string>,
    searchText: string = '',
    sortOrder: string = ''
  ): ISchemaObjectNode[] {
    if (conv.DatabaseType) {
      this.srcDbName = extractSourceDbName(conv.DatabaseType)
    }
    let spannerTableIds = Object.keys(conv.SpSchema).filter((tableId: string) =>
      conv.SpSchema[tableId].Name.toLocaleLowerCase().includes(searchText.toLocaleLowerCase())
    )

    let spannerSequenceIds = Object.keys(conv.SpSequences).filter((seqId: string) =>
      conv.SpSequences[seqId].Name.toLocaleLowerCase().includes(searchText.toLocaleLowerCase())
    )

    let deletedTableIds = Object.keys(conv.SrcSchema).filter((tableId: string) => {
      return (
        spannerTableIds.indexOf(tableId) == -1 &&
        conv.SrcSchema[tableId].Name.replace(/[^A-Za-z0-9_]/g, '_').includes(
          searchText.toLocaleLowerCase()
        )
      )
    })
    let deletedIndexes = this.getDeletedIndexes(conv)

    let parentNode: ISchemaObjectNode = {
      name: `Tables (${spannerTableIds.length})`,
      type: ObjectExplorerNodeType.Tables,
      parent: '',
      pos: -1,
      isSpannerNode: true,
      id: '',
      parentId: '',
      children: spannerTableIds.map((tableId: string) => {
        let spannerTable = conv.SpSchema[tableId]
        return {
          name: spannerTable.Name,
          status: conversionRates[tableId],
          type: ObjectExplorerNodeType.Table,
          parent: spannerTable.ParentId != '' ? conv.SpSchema[spannerTable.ParentId]?.Name : '',
          pos: -1,
          isSpannerNode: true,
          id: tableId,
          parentId: spannerTable.ParentId,
          children: [
            {
              name: `Indexes (${spannerTable.Indexes ? spannerTable.Indexes.length : 0})`,
              status: '',
              type: ObjectExplorerNodeType.Indexes,
              parent: conv.SpSchema[tableId].Name,
              pos: -1,
              isSpannerNode: true,
              id: '',
              parentId: tableId,
              children: spannerTable.Indexes
                ? spannerTable.Indexes.map((index: ICreateIndex, i: number) => {
                    return {
                      name: index.Name,
                      type: ObjectExplorerNodeType.Index,
                      parent: conv.SpSchema[tableId].Name,
                      pos: i,
                      isSpannerNode: true,
                      id: index.Id,
                      parentId: tableId,
                    }
                  })
                : [],
            },
          ],
        }
      }),
    }

    deletedTableIds.forEach((tableId: string) => {
      parentNode.children?.push({
        name: conv.SrcSchema[tableId].Name.replace(/[^A-Za-z0-9_]/g, '_'),
        status: 'DARK',
        type: ObjectExplorerNodeType.Table,
        pos: -1,
        isSpannerNode: true,
        children: [],
        isDeleted: true,
        id: tableId,
        parent: '',
        parentId: '',
      })
    })

    // add deleted indexes
    parentNode.children?.forEach((tableNode: ISchemaObjectNode, i: number) => {
      if (deletedIndexes[tableNode.id]) {
        deletedIndexes[tableNode.id].forEach((index: IIndex) => {
          parentNode.children![i].children![0].children?.push({
            name: index.Name.replace(/[^A-Za-z0-9_]/g, '_'),
            type: ObjectExplorerNodeType.Index,
            parent: conv.SpSchema[tableNode.name]?.Name,
            pos: i,
            isSpannerNode: true,
            isDeleted: true,
            id: index.Id,
            parentId: tableNode.id,
          })
        })
      }
    })

    let sequenceNode: ISchemaObjectNode = {
      name: `Sequences (${spannerSequenceIds.length})`,
      type: ObjectExplorerNodeType.Sequences,
      parent: '',
      pos: -1,
      isSpannerNode: true,
      id: '',
      parentId: '',
      children: spannerSequenceIds.map((seqId: string) => {
        let spannerSequence = conv.SpSequences[seqId]
        return {
          name: spannerSequence.Name,
          status: '',
          type: ObjectExplorerNodeType.Sequence,
          parent: '',
          pos: -1,
          isSpannerNode: true,
          id: seqId,
          parentId: '',
          children: [],
        }
      }),
    }

    if (sortOrder === 'asc' || sortOrder === '') {
      parentNode.children?.sort((a, b) => (a.name > b.name ? 1 : b.name > a.name ? -1 : 0))
      sequenceNode.children?.sort((a, b) => (a.name > b.name ? 1 : b.name > a.name ? -1 : 0))
    } else if (sortOrder === 'desc') {
      parentNode.children?.sort((a, b) => (b.name > a.name ? 1 : a.name > b.name ? -1 : 0))
      sequenceNode.children?.sort((a, b) => (b.name > a.name ? 1 : a.name > b.name ? -1 : 0))
    }

    let mainNodeChildren :ISchemaObjectNode[] = [parentNode]
    if (autoGenSupportedDbs.includes(this.srcDbName)) {
      mainNodeChildren.push(sequenceNode)
    }
    return [
      {
        name: conv.DatabaseName,
        children: mainNodeChildren,
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
    let srcTableIds = Object.keys(conv.SrcSchema).filter((tableId: string) =>
      conv.SrcSchema[tableId].Name.toLocaleLowerCase().includes(searchText.toLocaleLowerCase())
    )

    let parentNode: ISchemaObjectNode = {
      name: `Tables (${srcTableIds.length})`,
      type: ObjectExplorerNodeType.Tables,
      pos: -1,
      isSpannerNode: false,
      id: '',
      parent: '',
      parentId: '',
      children: srcTableIds.map((tableId: string) => {
        let srcTable = conv.SrcSchema[tableId]
        return {
          name: srcTable.Name,
          status: conversionRates[tableId] ? conversionRates[tableId] : 'NONE',
          type: ObjectExplorerNodeType.Table,
          parent: '',
          pos: -1,
          isSpannerNode: false,
          id: tableId,
          parentId: '',
          children: [
            {
              name: `Indexes (${srcTable.Indexes?.length || '0'})`,
              status: '',
              type: ObjectExplorerNodeType.Indexes,
              parent: '',
              pos: -1,
              isSpannerNode: false,
              id: '',
              parentId: '',
              children: srcTable.Indexes
                ? srcTable.Indexes.map((index: IIndex, i: number) => {
                    return {
                      name: index.Name,
                      type: ObjectExplorerNodeType.Index,
                      parent: conv.SrcSchema[tableId].Name,
                      isSpannerNode: false,
                      pos: i,
                      id: index.Id,
                      parentId: tableId,
                    }
                  })
                : [],
            },
          ],
        }
      }),
    }
    if (sortOrder === 'asc' || sortOrder === '') {
      parentNode.children?.sort((a, b) => (a.name > b.name ? 1 : b.name > a.name ? -1 : 0))
    } else if (sortOrder === 'desc') {
      parentNode.children?.sort((a, b) => (b.name > a.name ? 1 : a.name > b.name ? -1 : 0))
    }

    return [
      {
        name: conv.DatabaseName,
        children: [parentNode],
        type: ObjectExplorerNodeType.DbName,
        isSpannerNode: false,
        parent: '',
        pos: -1,
        id: '',
        parentId: '',
      },
    ]
  }

  getColumnMapping(tableId: string, data: IConv): IColumnTabData[] {
    let spTableName = this.getSpannerTableNameFromId(tableId, data)
    let srcColIds = data.SrcSchema[tableId].ColIds
    let spColIds = data.SpSchema[tableId] ? data.SpSchema[tableId].ColIds : null
    let srcPks = data.SrcSchema[tableId].PrimaryKeys
    let spPks = spColIds ? data.SpSchema[tableId].PrimaryKeys : null
    let standardTypeToPGSQLTypeMap: Map<String, String>
    this.standardTypeToPGSQLTypeMap.subscribe((typemap) => {
      standardTypeToPGSQLTypeMap = typemap
    })
    const spColMax = ColLength.StorageMaxLength
    const res: IColumnTabData[] = data.SrcSchema[tableId].ColIds.map((colId: string, i: number) => {
      let spPkOrder
      if (spTableName) {
        data.SpSchema[tableId].PrimaryKeys.forEach((pk: IIndexKey) => {
          if (pk.ColId == colId) {
            spPkOrder = pk.Order
          }
        })
      }
      let spannerColDef = spTableName ? data.SpSchema[tableId]?.ColDefs[colId] : null
      let pgSQLDatatype = spannerColDef ? standardTypeToPGSQLTypeMap.get(spannerColDef.T.Name) : ''
      return {
        spOrder: spannerColDef ? i + 1 : '',
        srcOrder: i + 1,
        spColName: spannerColDef ? spannerColDef.Name : '',
        spDataType: spannerColDef ? (data.SpDialect === Dialect.PostgreSQLDialect ? (pgSQLDatatype === undefined ? spannerColDef.T.Name : pgSQLDatatype) : spannerColDef.T.Name) : '',
        srcColName: data.SrcSchema[tableId].ColDefs[colId].Name,
        srcDataType: data.SrcSchema[tableId].ColDefs[colId].Type.Name,
        spIsPk:
          spannerColDef && spTableName
            ? data.SpSchema[tableId].PrimaryKeys?.map((pk) => pk.ColId).indexOf(colId) !== -1
            : false,
        srcIsPk: srcPks ? srcPks.map((pk) => pk.ColId).indexOf(colId) !== -1 : false,
        spIsNotNull: spannerColDef && spTableName ? spannerColDef.NotNull : false,
        srcIsNotNull: data.SrcSchema[tableId].ColDefs[colId].NotNull,
        srcId: colId,
        spId: spannerColDef ? colId : '',
        spColMaxLength: spannerColDef?.T.Len != 0 ? (spannerColDef?.T.Len != spColMax ? spannerColDef?.T.Len: 'MAX') : '',
        srcColMaxLength: data.SrcSchema[tableId].ColDefs[colId].Type.Mods != null ? data.SrcSchema[tableId].ColDefs[colId].Type.Mods[0] : '',
        spAutoGen: spannerColDef?.AutoGen != null ? spannerColDef?.AutoGen : {
          Name: '',
          GenerationType: ''
        },
      }
    })
    if (spColIds) {
      spColIds.forEach((colId: string, i: number) => {
        if (srcColIds.indexOf(colId) < 0) {
          let spColumn = data.SpSchema[tableId].ColDefs[colId]
          let spannerColDef = spTableName ? data.SpSchema[tableId]?.ColDefs[colId] : null
          let pgSQLDatatype = spannerColDef ? standardTypeToPGSQLTypeMap.get(spannerColDef.T.Name) : ''
          res.push({
            spOrder: i + 1,
            srcOrder: '',
            spColName: spColumn.Name,
            spDataType: spannerColDef ? (data.SpDialect === Dialect.PostgreSQLDialect ? (pgSQLDatatype === undefined ? spannerColDef.T.Name : pgSQLDatatype) : spannerColDef.T.Name) : '',
            srcColName: '',
            srcDataType: '',
            spIsPk: spPks ? spPks.map((p) => p.ColId).indexOf(colId) !== -1 : false,
            srcIsPk: false,
            spIsNotNull: spColumn.NotNull,
            srcIsNotNull: false,
            srcId: '',
            spId: colId,
            srcColMaxLength: '',
            spColMaxLength: spannerColDef?.T.Len,
            spAutoGen: spColumn.AutoGen
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
    let srcFks = data.SrcSchema[id]?.ForeignKeys
    if (!srcFks) {
      return []
    }
    return srcFks.map((srcFk: ISpannerForeignKey) => {
      let spFk = this.getSpannerFkFromId(data, id, srcFk.Id)
      let spColumns = spFk
        ? spFk.ColIds.map((columnId: string) => {
            return data.SpSchema[id].ColDefs[columnId].Name
          })
        : []
      let spColIds = spFk ? spFk.ColIds : []
      let srcColumns = srcFk.ColIds.map((columnId: string) => {
        return data.SrcSchema[id].ColDefs[columnId].Name
      })
      let spReferColumns = spFk
        ? spFk.ReferColumnIds.map((referColId: string) => {
            return data.SpSchema[srcFk.ReferTableId].ColDefs[referColId].Name
          })
        : []
      let spReferColumnIds = spFk ? spFk.ReferColumnIds : []
      let srcReferColumns = srcFk.ReferColumnIds.map((referColId: string) => {
        return data.SrcSchema[srcFk.ReferTableId].ColDefs[referColId].Name
      })

      return {
        srcFkId: srcFk.Id,
        spFkId: spFk?.Id,
        spName: spFk ? spFk.Name : '',
        srcName: srcFk.Name,
        spColumns: spColumns,
        srcColumns: srcColumns,
        spReferTable: spFk ? data.SpSchema[spFk.ReferTableId].Name : '',
        srcReferTable: data.SrcSchema[srcFk.ReferTableId].Name,
        spReferColumns: spReferColumns,
        srcReferColumns: srcReferColumns,
        spColIds: spColIds,
        spReferColumnIds: spReferColumnIds,
        spReferTableId: spFk ? spFk.ReferTableId : '',
      }
    })
  }

  getIndexMapping(tableId: string, data: IConv, indexId: string): IIndexData[] {
    let srcIndex = this.getSourceIndexFromId(data, tableId, indexId)
    let spIndex = this.getSpannerIndexFromId(data, tableId, indexId)

    let srcIndexKeyColIds: string[] = srcIndex
      ? srcIndex.Keys.map((indexKey: ISrcIndexKey) => indexKey.ColId)
      : []
    let spIndexKeyColIds: string[] = spIndex
      ? spIndex.Keys.map((indexKey: IIndexKey) => indexKey.ColId)
      : []
    let indexData: Array<IIndexData> = srcIndex
      ? srcIndex.Keys.map((srcIndexKey: ISrcIndexKey) => {
          let spIndexKey: IIndexKey | null = this.getSpannerIndexKeyFromColId(
            data,
            tableId,
            indexId,
            srcIndexKey.ColId
          )
          return {
            srcColId: srcIndexKey.ColId,
            spColId: spIndexKey ? spIndexKey.ColId : undefined,
            srcColName: data.SrcSchema[tableId].ColDefs[srcIndexKey.ColId].Name,
            srcOrder: srcIndexKey.Order,
            srcDesc: srcIndexKey.Desc,
            spColName: spIndexKey ? data.SpSchema[tableId].ColDefs[spIndexKey.ColId].Name : '',
            spOrder: spIndexKey ? spIndexKey.Order : undefined,
            spDesc: spIndexKey ? spIndexKey.Desc : undefined,
          }
        })
      : []

    spIndexKeyColIds.forEach((spColId: string) => {
      if (srcIndexKeyColIds.indexOf(spColId) == -1) {
        let spIndexKey = this.getSpannerIndexKeyFromColId(data, tableId, indexId, spColId)
        indexData.push({
          srcColName: '',
          srcOrder: '',
          srcColId: undefined,
          srcDesc: undefined,
          spColName: data.SpSchema[tableId].ColDefs[spColId].Name,
          spOrder: spIndexKey ? spIndexKey.Order : undefined,
          spDesc: spIndexKey ? spIndexKey.Desc : undefined,
          spColId: spIndexKey ? spIndexKey.ColId : undefined,
        })
      }
    })
    return indexData
  }

  getSequenceMapping(seqId: string, data: IConv): ISequenceData {
    let srcSequence = null
    let spSequenceName = this.getSpannerSequenceNameFromId(seqId, data)
    let sequence: ISequenceData = {}
    if (spSequenceName != null) {
      let spSequence = data.SpSequences[seqId]
      sequence.spSeqName = spSequence.Name
      sequence.spSequenceKind = spSequence.SequenceKind
      sequence.spSkipRangeMax = spSequence.SkipRangeMax
      sequence.spSkipRangeMin = spSequence.SkipRangeMin
      sequence.spStartWithCounter = spSequence.StartWithCounter
    }
    return sequence
  }

  getSpannerFkFromId(conv: IConv, tableId: string, srcFkId: string): IForeignKey | null {
    let spFk: IForeignKey | null = null
    conv.SpSchema[tableId]?.ForeignKeys?.forEach((fk: IForeignKey) => {
      if (fk.Id == srcFkId) {
        spFk = fk
      }
    })
    return spFk
  }

  getSourceIndexFromId(conv: IConv, tableId: string, indexId: string): IIndex | null {
    let srcIndex: IIndex | null = null
    conv.SrcSchema[tableId]?.Indexes?.forEach((index: IIndex) => {
      if (index.Id == indexId) {
        srcIndex = index
      }
    })
    return srcIndex
  }

  getSpannerIndexFromId(conv: IConv, tableId: string, indexId: string): ICreateIndex | null {
    let spIndex: ICreateIndex | null = null
    conv.SpSchema[tableId]?.Indexes?.forEach((index: ICreateIndex) => {
      if (index.Id == indexId) {
        spIndex = index
      }
    })
    return spIndex
  }

  getSpannerSequenceNameFromId(id: string, conv: IConv): string | null {
    let spSeqName: string | null = null
    Object.keys(conv.SpSequences).forEach((key: string) => {
      if (conv.SpSequences[key].Id === id) {
        spSeqName = conv.SpSequences[key].Name
      }
    })
    return spSeqName
  }

  getSpannerIndexKeyFromColId(
    conv: IConv,
    tableId: string,
    indexId: string,
    colId: string
  ): IIndexKey | null {
    let indexKey: IIndexKey | null = null

    let indexes = conv.SpSchema[tableId]?.Indexes
      ? conv.SpSchema[tableId].Indexes.filter((index: IIndex) => {
          return index.Id == indexId
        })
      : null

    if (indexes && indexes.length > 0) {
      let indexKeys = indexes[0].Keys.filter((key: IIndexKey) => {
        return key.ColId == colId
      })
      indexKey = indexKeys.length > 0 ? indexKeys[0] : null
    }
    return indexKey
  }

  getSourceIndexKeyFromColId(
    conv: IConv,
    tableId: string,
    indexId: string,
    colId: string
  ): ISrcIndexKey | null {
    let indexKey: ISrcIndexKey | null = null

    let indexes = conv.SrcSchema[tableId]?.Indexes
      ? conv.SrcSchema[tableId].Indexes.filter((index: IIndex) => {
          return index.Id == indexId
        })
      : null

    if (indexes && indexes.length > 0) {
      let indexKeys = indexes[0].Keys.filter((key: IIndexKey) => {
        return key.ColId == colId
      })
      indexKey = indexKeys.length > 0 ? indexKeys[0] : null
    }
    return indexKey
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
  getTableIdFromSpName(name: string, conv: IConv): string {
    let tableId: string = ''
    Object.keys(conv.SpSchema).forEach((key: string) => {
      if (conv.SpSchema[key].Name === name) {
        tableId = conv.SpSchema[key].Id
      }
    })
    return tableId
  }
  getColIdFromSpannerColName(name: string, tableId: string, conv: IConv): string {
    let colId: string = ''
    Object.keys(conv.SpSchema[tableId].ColDefs).forEach((key: string) => {
      if (conv.SpSchema[tableId].ColDefs[key].Name === name) {
        colId = conv.SpSchema[tableId].ColDefs[key].Id
      }
    })
    return colId
  }

  getDeletedIndexes(conv: IConv): Record<string, IIndex[]> {
    let deletedIndexes: Record<string, IIndex[]> = {}
    Object.keys(conv.SpSchema).forEach((tableId: string) => {
      let spTable = conv.SpSchema[tableId]
      let srcTable = conv.SrcSchema[tableId]
      let spIndexIds: string[] =
        spTable && spTable.Indexes
          ? spTable.Indexes.map((index: ICreateIndex) => {
              return index.Id
            })
          : []

      let tableDeletedIndexes =
        srcTable && srcTable.Indexes
          ? srcTable.Indexes?.filter((index: IIndex) => {
              if (!spIndexIds.includes(index.Id)) {
                return true
              }
              return false
            })
          : null
      if (spTable && srcTable && tableDeletedIndexes && tableDeletedIndexes.length > 0) {
        deletedIndexes[tableId] = tableDeletedIndexes
      }
    })

    return deletedIndexes
  }
}
