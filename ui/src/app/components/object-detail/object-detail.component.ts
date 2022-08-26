import { Component, EventEmitter, Input, OnInit, Output, SimpleChanges } from '@angular/core'
import { FormArray, FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms'
import IUpdateTable from '../../model/update-table'
import { DataService } from 'src/app/services/data/data.service'
import { MatDialog } from '@angular/material/dialog'
import { InfodialogComponent } from '../infodialog/infodialog.component'
import IColumnTabData, { IIndexData } from '../../model/edit-table'
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service'
import IFkTabData from 'src/app/model/fk-tab-data'
import { ObjectExplorerNodeType, StorageKeys } from 'src/app/app.constants'
import FlatNode from 'src/app/model/schema-object-node'
import { Subscription, take } from 'rxjs'
import { MatTabChangeEvent } from '@angular/material/tabs/tab-group'
import IConv, { ICreateIndex, IPrimaryKey } from 'src/app/model/conv'
import { ConversionService } from 'src/app/services/conversion/conversion.service'
import { DropIndexOrTableDialogComponent } from '../drop-index-or-table-dialog/drop-index-or-table-dialog.component'

@Component({
  selector: 'app-object-detail',
  templateUrl: './object-detail.component.html',
  styleUrls: ['./object-detail.component.scss'],
})
export class ObjectDetailComponent implements OnInit {
  constructor(
    private data: DataService,
    private dialog: MatDialog,
    private snackbar: SnackbarService,
    private conversion: ConversionService
  ) {}

  @Input() currentObject: FlatNode | null = null
  @Input() typeMap: any = {}
  @Input() ddlStmts: any = {}
  @Input() fkData: IFkTabData[] = []
  @Input() tableData: IColumnTabData[] = []
  @Input() currentDatabase: string = 'spanner'
  @Input() indexData: IIndexData[] = []
  @Output() updateSidebar = new EventEmitter<boolean>()
  ObjectExplorerNodeType = ObjectExplorerNodeType
  conv: IConv = {} as IConv
  interleaveObj!: Subscription
  interleaveStatus: any
  interleaveParentName: string | null = null

  ngOnInit(): void {
    this.data.conv.subscribe({
      next: (res: IConv) => {
        this.conv = res
      },
    })
  }

  displayedColumns = [
    'srcOrder',
    'srcColName',
    'srcDataType',
    'srcIsPk',
    'srcIsNotNull',
    'spOrder',
    'spColName',
    'spDataType',
    'spIsPk',
    'spIsNotNull',
    'dropButton',
  ]
  displayedFkColumns = [
    'srcName',
    'srcColumns',
    'srcReferTable',
    'srcReferColumns',
    'spName',
    'spColumns',
    'spReferTable',
    'spReferColumns',
    'dropButton',
  ]
  displayedPkColumns = [
    'srcOrder',
    'srcColName',
    'srcDataType',
    'srcIsPk',
    'srcIsNotNull',
    'spOrder',
    'spColName',
    'spDataType',
    'spIsPk',
    'spIsNotNull',
    'dropButton',
  ]

  indexDisplayedColumns = [
    'srcIndexColName',
    'srcSortBy',
    'srcIndexOrder',
    'spIndexColName',
    'spSortBy',
    'spIndexOrder',
    'dropButton',
  ]
  dataSource: any = []
  fkDataSource: any = []
  pkDataSource: any = []
  pkData: IColumnTabData[] = []
  isPkEditMode: boolean = false
  isEditMode: boolean = false
  isFkEditMode: boolean = false
  isIndexEditMode: boolean = false
  isObjectSelected: boolean = false
  rowArray: FormArray = new FormArray([])
  pkArray: FormArray = new FormArray([])
  fkArray: FormArray = new FormArray([])
  srcDbName: string = localStorage.getItem(StorageKeys.SourceDbName) as string
  isSpTableSuggesstionDisplay: boolean[] = []
  spTableSuggestion: string[] = []
  currentTabIndex: number = 0
  indexColumnNames: string[] = []
  addIndexKeyForm = new FormGroup({
    columnName: new FormControl('', [Validators.required]),
    ascOrDesc: new FormControl('', [Validators.required]),
  })
  addedPkColumnName: string = ''
  pkColumnNames: string[] = []
  addPkColumnForm = new FormGroup({
    columnName: new FormControl('', [Validators.required]),
  })
  pkObj: IPrimaryKey = {} as IPrimaryKey

  ngOnChanges(changes: SimpleChanges): void {
    this.fkData = changes['fkData']?.currentValue || this.fkData
    this.currentObject = changes['currentObject']?.currentValue || this.currentObject
    this.tableData = changes['tableData']?.currentValue || this.tableData
    this.indexData = changes['indexData']?.currentValue || this.indexData
    this.currentDatabase = changes['currentDatabase']?.currentValue || this.currentDatabase
    this.currentTabIndex = this.currentObject?.type === ObjectExplorerNodeType.Table ? 0 : -1
    this.isObjectSelected = this.currentObject ? true : false
    this.isEditMode = false
    this.isFkEditMode = false
    this.isPkEditMode = false
    this.rowArray = new FormArray([])
    this.pkData = this.conversion.getPkMapping(this.tableData)
    this.pkColumnNames = []
    this.interleaveParentName = this.getParentFromDdl()

    if (this.currentObject?.type === ObjectExplorerNodeType.Table) {
      this.setPkOrder()
      this.checkIsInterleave()
      this.interleaveObj = this.data.tableInterleaveStatus.subscribe((res) => {
        this.interleaveStatus = res
      })
      this.tableData.forEach((row) => {
        this.rowArray.push(
          new FormGroup({
            srcOrder: new FormControl(row.srcOrder),
            srcColName: new FormControl(row.srcColName),
            srcDataType: new FormControl(row.srcDataType),
            srcIsPk: new FormControl(row.srcIsPk),
            srcIsNotNull: new FormControl(row.srcIsNotNull),
            spOrder: new FormControl(row.spOrder),
            spColName: new FormControl(row.spColName),
            spDataType: new FormControl(row.spDataType),
            spIsPk: new FormControl(row.spIsPk),
            spIsNotNull: new FormControl(row.spIsNotNull),
          })
        )
      })
    } else if (this.currentObject) {
      this.checkIsInterleave()
      this.setIndexRows()
    }

    this.dataSource = this.rowArray.controls
    this.updateSpTableSuggestion()

    this.setAddPkColumnList()
    this.setPkRows()

    this.fkArray = new FormArray([])
    this.fkData.forEach((fk) => {
      this.fkArray.push(
        new FormGroup({
          spName: new FormControl(fk.spName),
          srcName: new FormControl(fk.srcName),
          spColumns: new FormControl(fk.spColumns),
          srcColumns: new FormControl(fk.srcColumns),
          spReferTable: new FormControl(fk.spReferTable),
          srcReferTable: new FormControl(fk.srcReferTable),
          spReferColumns: new FormControl(fk.spReferColumns),
          srcReferColumns: new FormControl(fk.srcReferColumns),
        })
      )
    })
    this.fkDataSource = this.fkArray.controls

    this.data.getSummary()
  }

  toggleEdit() {
    this.currentTabIndex = 0
    if (this.isEditMode) {
      let updateData: IUpdateTable = { UpdateCols: {} }
      this.rowArray.value.forEach((col: IColumnTabData, i: number) => {
        let oldRow = this.tableData[i]
        updateData.UpdateCols[this.tableData[i].spColName] = {
          Rename: oldRow.spColName !== col.spColName ? col.spColName : '',
          NotNull: col.spIsNotNull ? 'ADDED' : 'REMOVED',
          PK: '',
          Removed: false,
          ToType: oldRow.spDataType !== col.spDataType ? col.spDataType : '',
        }
      })
      this.data.updateTable(this.currentObject!.name, updateData).subscribe({
        next: (res: string) => {
          if (res == '') {
            this.isEditMode = false
          } else {
            this.dialog.open(InfodialogComponent, {
              data: { message: res, type: 'error' },
              maxWidth: '500px',
            })
          }
        },
      })
    } else {
      this.isEditMode = true
    }
  }

  dropColumn(element: any) {
    let colName = element.get('spColName').value
    let updateData: IUpdateTable = { UpdateCols: {} }
    this.rowArray.value.forEach((col: IColumnTabData, i: number) => {
      updateData.UpdateCols[this.tableData[i].spColName] = {
        Rename: '',
        NotNull: '',
        PK: '',
        Removed: col.spColName === colName ? true : false,
        ToType: '',
      }
    })
    this.data.updateTable(this.currentObject!.name, updateData).subscribe({
      next: (res: string) => {
        if (res == '') {
          this.data.getDdl()
          this.snackbar.openSnackBar(`${colName} column dropped successfully`, 'Close', 5)
        } else {
          this.dialog.open(InfodialogComponent, {
            data: { message: res, type: 'error' },
            maxWidth: '500px',
          })
        }
      },
    })
  }

  updateSpTableSuggestion() {
    this.isSpTableSuggesstionDisplay = []
    this.spTableSuggestion = []
    this.tableData.forEach((item: any) => {
      const srDataType = item.srcDataType
      const spDataType = item.spDataType
      let brief: string = ''
      this.typeMap[srDataType]?.forEach((type: any) => {
        if (spDataType == type.T) brief = type.Brief
      })
      this.isSpTableSuggesstionDisplay.push(brief !== '')
      this.spTableSuggestion.push(brief)
    })
  }
  spTableEditSuggestionHandler(index: number, spDataType: string) {
    const srDataType = this.tableData[index].srcDataType
    let brief: string = ''
    this.typeMap[srDataType].forEach((type: any) => {
      if (spDataType == type.T) brief = type.Brief
    })
    this.isSpTableSuggesstionDisplay[index] = brief !== ''
    this.spTableSuggestion[index] = brief
  }

  setPkRows() {
    this.pkArray = new FormArray([])
    this.pkOrderValidation()
    var srcArr = new Array()
    var spArr = new Array()
    this.pkData.forEach((row) => {
      if (row.srcIsPk) {
        srcArr.push({
          srcColName: row.srcColName,
          srcDataType: row.srcDataType,
          srcIsNotNull: row.srcIsNotNull,
          srcIsPk: row.srcIsPk,
          srcOrder: row.srcOrder,
        })
      }
      if (row.spIsPk) {
        spArr.push({
          spColName: row.spColName,
          spDataType: row.spDataType,
          spIsNotNull: row.spIsNotNull,
          spIsPk: row.spIsPk,
          spOrder: row.spOrder,
        })
      }
    })

    spArr.sort((a, b) => {
      return a.spOrder - b.spOrder
    })

    for (let i = 0; i < Math.min(srcArr.length, spArr.length); i++) {
      this.pkArray.push(
        new FormGroup({
          srcOrder: new FormControl(srcArr[i].srcOrder),
          srcColName: new FormControl(srcArr[i].srcColName),
          srcDataType: new FormControl(srcArr[i].srcDataType),
          srcIsPk: new FormControl(srcArr[i].srcIsPk),
          srcIsNotNull: new FormControl(srcArr[i].srcIsNotNull),
          spOrder: new FormControl(spArr[i].spOrder),
          spColName: new FormControl(spArr[i].spColName),
          spDataType: new FormControl(spArr[i].spDataType),
          spIsPk: new FormControl(spArr[i].spIsPk),
          spIsNotNull: new FormControl(spArr[i].spIsNotNull),
        })
      )
    }
    if (srcArr.length > Math.min(srcArr.length, spArr.length))
      for (let i = Math.min(srcArr.length, spArr.length); i < srcArr.length; i++) {
        this.pkArray.push(
          new FormGroup({
            srcOrder: new FormControl(srcArr[i].srcOrder),
            srcColName: new FormControl(srcArr[i].srcColName),
            srcDataType: new FormControl(srcArr[i].srcDataType),
            srcIsPk: new FormControl(srcArr[i].srcIsPk),
            srcIsNotNull: new FormControl(srcArr[i].srcIsNotNull),
            spOrder: new FormControl(''),
            spColName: new FormControl(''),
            spDataType: new FormControl(''),
            spIsPk: new FormControl(false),
            spIsNotNull: new FormControl(false),
          })
        )
      }
    else if (spArr.length > Math.min(srcArr.length, spArr.length))
      for (let i = Math.min(srcArr.length, spArr.length); i < spArr.length; i++) {
        this.pkArray.push(
          new FormGroup({
            srcOrder: new FormControl(''),
            srcColName: new FormControl(''),
            srcDataType: new FormControl(''),
            srcIsPk: new FormControl(false),
            srcIsNotNull: new FormControl(false),
            spOrder: new FormControl(spArr[i].spOrder),
            spColName: new FormControl(spArr[i].spColName),
            spDataType: new FormControl(spArr[i].spDataType),
            spIsPk: new FormControl(spArr[i].spIsPk),
            spIsNotNull: new FormControl(spArr[i].spIsNotNull),
          })
        )
      }
    this.pkDataSource = this.pkArray.controls
  }

  setPkColumn(columnName: string) {
    this.addedPkColumnName = columnName
  }

  addPkColumn() {
    let index = this.tableData.map((item) => item.spColName).indexOf(this.addedPkColumnName)
    let newColumnOrder = 1
    this.tableData[index].spIsPk = true
    this.pkData = []
    this.pkData = this.conversion.getPkMapping(this.tableData)
    index = this.pkData.findIndex((item) => item.srcOrder === index + 1)
    this.pkArray.value.forEach((pk: IColumnTabData) => {
      if (pk.spIsPk) {
        newColumnOrder = newColumnOrder + 1
      }
      for (let i = 0; i < this.pkData.length; i++) {
        if (this.pkData[i].spColName == pk.spColName) {
          this.pkData[i].spOrder = pk.spOrder
          break
        }
      }
    })
    this.pkData[index].spOrder = newColumnOrder
    this.setAddPkColumnList()
    this.setPkRows()
  }

  setAddPkColumnList() {
    this.pkColumnNames = []
    let currentPkColumns: string[] = []
    this.pkData.forEach((row) => {
      if (row.spIsPk) {
        currentPkColumns.push(row.spColName)
      }
    })
    for (let i = 0; i < this.tableData.length; i++) {
      if (!currentPkColumns.includes(this.tableData[i].spColName))
        this.pkColumnNames.push(this.tableData[i].spColName)
    }
  }

  setPkOrder() {
    if (
      this.currentObject &&
      this.conv.SpSchema[this.currentObject!.name]?.Pks.length == this.pkData.length
    ) {
      this.pkData.forEach((pk: IColumnTabData, i: number) => {
        if (this.pkData[i].spColName === this.conv.SpSchema[this.currentObject!.name].Pks[i].Col) {
          this.pkData[i].spOrder = this.conv.SpSchema[this.currentObject!.name].Pks[i].Order
        } else {
          let index = this.conv.SpSchema[this.currentObject!.name].Pks.map(
            (item) => item.Col
          ).indexOf(pk.spColName)
          pk.spOrder = this.conv.SpSchema[this.currentObject!.name].Pks[index].Order
        }
      })
    } else {
      this.pkData.forEach((pk: IColumnTabData, i: number) => {
        let index = this.conv.SpSchema[this.currentObject!.name]?.Pks.map(
          (item) => item.Col
        ).indexOf(pk.spColName)
        if (index !== -1) {
          pk.spOrder = this.conv.SpSchema[this.currentObject!.name]?.Pks[index].Order
        }
      })
    }
  }

  pkOrderValidation() {
    let arr = this.pkData.map((item) => Number(item.spOrder))
    arr.sort()
    if (arr[arr.length - 1] > arr.length) {
      arr.forEach((num: number, ind: number) => {
        this.pkData.forEach((pk: IColumnTabData) => {
          if (pk.spOrder == num) {
            pk.spOrder = ind + 1
          }
        })
      })
    }
    if (arr[0] == 0 && arr[arr.length - 1] <= arr.length) {
      let missingOrder: number
      for (let i = 0; i < arr.length; i++) {
        if (arr[i] != i) {
          missingOrder = i
          break
        }
        missingOrder = arr.length
      }
      this.pkData.forEach((pk: IColumnTabData) => {
        if (pk.spOrder < missingOrder) {
          pk.spOrder = Number(pk.spOrder) + 1
        }
      })
    }
  }

  getPkRequestObj() {
    let tableId: string = this.conv.SpSchema[this.currentObject!.name].Id
    let Columns: { ColumnId: string; ColName: string; Desc: boolean; Order: number }[] = []
    this.pkData.forEach((row: IColumnTabData) => {
      if (row.spIsPk)
        Columns.push({
          ColumnId: this.conv.SpSchema[this.currentObject!.name].ColDefs[row.spColName].Id,
          ColName: row.spColName,
          Desc:
            typeof this.conv.SpSchema[this.currentObject!.name].Pks.find(
              ({ Col }) => Col === row.spColName
            ) !== 'undefined'
              ? this.conv.SpSchema[this.currentObject!.name].Pks.find(
                  ({ Col }) => Col === row.spColName
                )!.Desc
              : false,
          Order: parseInt(row.spOrder as string),
        })
    })
    this.pkObj.TableId = tableId
    this.pkObj.Columns = Columns
  }

  togglePkEdit() {
    this.currentTabIndex = 1
    if (this.isPkEditMode) {
      this.pkArray.value.forEach((pk: IColumnTabData) => {
        for (let i = 0; i < this.pkData.length; i++) {
          if (pk.spColName == this.pkData[i].spColName) {
            this.pkData[i].spOrder = pk.spOrder
            break
          }
        }
      })
      this.pkOrderValidation()

      this.getPkRequestObj()
      if (this.pkObj.Columns.length == 0) {
        this.dialog.open(InfodialogComponent, {
          data: { message: 'Add columns to the primary key for saving', type: 'error' },
          maxWidth: '500px',
        })
      }

      this.isPkEditMode = false
      this.data.updatePk(this.pkObj).subscribe({
        next: (res: string) => {
          if (res == '') {
            this.isEditMode = false
          } else {
            this.dialog.open(InfodialogComponent, {
              data: { message: res, type: 'error' },
              maxWidth: '500px',
            })
            this.isPkEditMode = true
          }
        },
      })
    } else {
      this.isPkEditMode = true
    }
  }

  dropPk(element: any) {
    let index = this.tableData.map((item) => item.spColName).indexOf(element.value.spColName)
    let removedOrder = element.value.spOrder
    this.tableData[index].spIsPk = false
    this.pkData = []
    this.pkData = this.conversion.getPkMapping(this.tableData)
    this.pkArray.value.forEach((pk: IColumnTabData) => {
      for (let i = 0; i < this.pkData.length; i++) {
        if (pk.spColName == this.pkData[i].spColName) {
          this.pkData[i].spOrder = pk.spOrder
          break
        }
      }
    })

    this.pkData.forEach((column: IColumnTabData, ind: number) => {
      if (column.spOrder > removedOrder) {
        column.spOrder = Number(column.spOrder) - 1
      }
    })

    this.setAddPkColumnList()
    this.setPkRows()
  }

  toggleFkEdit() {
    this.currentTabIndex = 2
    if (this.isFkEditMode) {
      let updatedFkNames: Record<string, string> = {}

      this.fkArray.value.forEach((fk: IFkTabData, i: number) => {
        let oldFk = this.fkData[i]
        if (oldFk.spName !== fk.spName) {
          updatedFkNames[oldFk.spName] = fk.spName
        }
      })

      this.data.updateFkNames(this.currentObject!.name, updatedFkNames).subscribe({
        next: (res: string) => {
          if (res == '') {
            this.isFkEditMode = false
          } else {
            this.dialog.open(InfodialogComponent, {
              data: { message: res, type: 'error' },
              maxWidth: '500px',
            })
          }
        },
      })
    } else {
      this.currentTabIndex = 2
      this.isFkEditMode = true
    }
  }

  dropFk(element: any) {
    this.data.dropFk(this.currentObject!.name, element.get('spName').value).subscribe({
      next: (res: string) => {
        if (res == '') {
          this.data.getDdl()
          this.snackbar.openSnackBar(
            `${element.get('spName').value} Foreign key dropped successfully`,
            'Close',
            5
          )
        } else {
          this.dialog.open(InfodialogComponent, {
            data: { message: res, type: 'error' },
            maxWidth: '500px',
          })
        }
      },
    })
  }

  getRemovedFkIndex(element: any) {
    let ind: number = -1

    this.fkArray.value.forEach((fk: IFkTabData, i: number) => {
      if (fk.spName === element.get('spName').value) {
        ind = i
      }
    })
    return ind
  }
  convertToFk() {
    alert('Feature comming soon!')
  }

  checkIsInterleave() {
    if (this.currentObject) {
      this.data.getInterleaveConversionForATable(this.currentObject!.name)
    }
  }

  setInterleave() {
    this.data.setInterleave(this.currentObject!.name)
  }

  getParentFromDdl() {
    let substr: string = 'INTERLEAVE IN PARENT'
    let ddl: string = ''
    if (
      this.currentObject?.type === ObjectExplorerNodeType.Table &&
      this.ddlStmts[this.currentObject.name]?.includes(substr)
    ) {
      ddl = this.ddlStmts[this.currentObject.name].substring(
        this.ddlStmts[this.currentObject.name].indexOf(substr) + 20
      )
      return ddl.split(' ')[1]
    }
    return null
  }

  setIndexRows() {
    this.rowArray = new FormArray([])
    const addedIndexColumns: string[] = this.indexData
      .map((data) => (data.spColName ? data.spColName : ''))
      .filter((name) => name != '')
    this.indexColumnNames = this.conv.SpSchema[this.currentObject!.parent]?.ColNames.filter(
      (columnName) => {
        if (addedIndexColumns.includes(columnName)) {
          return false
        } else {
          return true
        }
      }
    )

    this.indexData.forEach((row: IIndexData) => {
      this.rowArray.push(
        new FormGroup({
          srcOrder: new FormControl(row.srcOrder),
          srcColName: new FormControl(row.srcColName),
          srcDesc: new FormControl(row.srcDesc),
          spOrder: new FormControl(row.spOrder),
          spColName: new FormControl(row.spColName),
          spDesc: new FormControl(row.spDesc),
        })
      )
    })
    this.dataSource = this.rowArray.controls
  }

  toggleIndexEdit() {
    if (this.isIndexEditMode) {
      let payload: ICreateIndex[] = []
      const tableName = this.currentObject?.parent || ''
      payload.push({
        Name: this.currentObject?.name || '',
        Table: this.currentObject?.parent || '',
        Unique: false,
        Keys: this.indexData
          .filter((idx) => {
            if (idx.spColName) return true
            return false
          })
          .map((col: any) => {
            return {
              Col: col.spColName,
              Desc: col.spDesc,
              Order: col.spOrder,
            }
          }),
        Id: '',
      })

      this.data.updateIndex(tableName, payload)
      this.addIndexKeyForm.controls['columnName'].setValue('')
      this.addIndexKeyForm.controls['ascOrDesc'].setValue('')
      this.addIndexKeyForm.markAsUntouched()
      this.data.getSummary()
      this.isIndexEditMode = false
    } else {
      this.isIndexEditMode = true
    }
  }

  dropIndex() {
    let openDialog = this.dialog.open(DropIndexOrTableDialogComponent, {
      width: '35vw',
      minWidth: '450px',
      maxWidth: '600px',
      data: { name: this.currentObject?.name, type: ObjectExplorerNodeType.Index },
    })
    openDialog.afterClosed().subscribe((res: string) => {
      if (res === ObjectExplorerNodeType.Index) {
        this.data
          .dropIndex(this.currentObject!.parent, this.currentObject!.name)
          .pipe(take(1))
          .subscribe((res: string) => {
            if (res === '') {
              this.isObjectSelected = false
              this.updateSidebar.emit(true)
            }
          })
        this.currentObject = null
      }
    })
  }
  dropIndexKey(index: number) {
    for (let i = 0; i < this.indexData.length; i++) {
      if (i === index || this.indexData[i].spColName === undefined) {
        this.indexData.splice(index, 1)
      }
    }
    this.setIndexRows()
  }

  addIndexKey() {
    let spIndexCount = 0
    this.indexData.forEach((idx) => {
      if (idx.spColName) spIndexCount += 1
    })
    this.indexData.push({
      spColName: this.addIndexKeyForm.value.columnName,
      spDesc: this.addIndexKeyForm.value.ascOrDesc === 'desc',
      spOrder: spIndexCount + 1,
      srcColName: '',
      srcDesc: undefined,
      srcOrder: '',
    })
    this.setIndexRows()
  }

  restoreSpannerTable() {
    let tableId = this.currentObject!.id
    this.data
      .restoreTable(tableId)
      .pipe(take(1))
      .subscribe((res: string) => {
        if (res === '') {
          this.isObjectSelected = false
        }
      })
    this.currentObject = null
  }

  dropTable() {
    let openDialog = this.dialog.open(DropIndexOrTableDialogComponent, {
      width: '35vw',
      minWidth: '450px',
      maxWidth: '600px',
      data: { name: this.currentObject?.name, type: ObjectExplorerNodeType.Table },
    })
    openDialog.afterClosed().subscribe((res: string) => {
      if (res === ObjectExplorerNodeType.Table) {
        let tableId = this.currentObject!.id
        this.data
          .dropTable(tableId)
          .pipe(take(1))
          .subscribe((res: string) => {
            if (res === '') {
              this.isObjectSelected = false
              this.updateSidebar.emit(true)
            }
          })
        this.currentObject = null
      }
    })
  }

  selectedColumnChange(tableName: string) {}

  tabChanged(tabChangeEvent: MatTabChangeEvent): void {
    this.currentTabIndex = tabChangeEvent.index
  }
}
