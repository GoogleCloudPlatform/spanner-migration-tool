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
import { empty, Subscription, take } from 'rxjs'
import { MatTabChangeEvent } from '@angular/material/tabs/tab-group'
import IConv, { ICreateIndex, IPrimaryKey } from 'src/app/model/conv'
import { ConversionService } from 'src/app/services/conversion/conversion.service'
import { DropIndexOrTableDialogComponent } from '../drop-index-or-table-dialog/drop-index-or-table-dialog.component'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'
import { TableUpdatePubSubService } from 'src/app/services/table-update-pub-sub/table-update-pub-sub.service'

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
    private conversion: ConversionService,
    private sidenav: SidenavService,
    private tableUpdatePubSub: TableUpdatePubSubService
  ) {}

  @Input() currentObject: FlatNode | null = null
  @Input() typeMap: any = {}
  @Input() ddlStmts: any = {}
  @Input() fkData: IFkTabData[] = []
  @Input() tableData: IColumnTabData[] = []
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

  srcDisplayedColumns = ['srcOrder', 'srcColName', 'srcDataType', 'srcIsPk', 'srcIsNotNull']

  spDisplayedColumns = ['spColName', 'spDataType', 'spIsPk', 'spIsNotNull', 'dropButton']
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
  spDataSource: any = []
  srcDataSource: any = []
  fkDataSource: any = []
  pkDataSource: any = []
  pkData: IColumnTabData[] = []
  isPkEditMode: boolean = false
  isEditMode: boolean = false
  isFkEditMode: boolean = false
  isIndexEditMode: boolean = false
  isObjectSelected: boolean = false
  srcRowArray: FormArray = new FormArray([])
  spRowArray: FormArray = new FormArray([])
  pkArray: FormArray = new FormArray([])
  fkArray: FormArray = new FormArray([])
  srcDbName: string = localStorage.getItem(StorageKeys.SourceDbName) as string
  isSpTableSuggesstionDisplay: boolean[] = []
  spTableSuggestion: string[] = []
  currentTabIndex: number = 0
  addedColumnName: string = ''
  droppedColumns: IColumnTabData[] = []
  pkColumnNames: string[] = []
  indexColumnNames: string[] = []
  addColumnForm = new FormGroup({
    columnName: new FormControl('', [Validators.required]),
  })
  addIndexKeyForm = new FormGroup({
    columnName: new FormControl('', [Validators.required]),
    ascOrDesc: new FormControl('', [Validators.required]),
  })
  addedPkColumnName: string = ''
  addPkColumnForm = new FormGroup({
    columnName: new FormControl('', [Validators.required]),
  })
  pkObj: IPrimaryKey = {} as IPrimaryKey

  ngOnChanges(changes: SimpleChanges): void {
    this.fkData = changes['fkData']?.currentValue || this.fkData
    this.currentObject = changes['currentObject']?.currentValue || this.currentObject
    this.tableData = changes['tableData']?.currentValue || this.tableData
    this.indexData = changes['indexData']?.currentValue || this.indexData
    this.currentTabIndex = this.currentObject?.type === ObjectExplorerNodeType.Table ? 0 : -1
    this.isObjectSelected = this.currentObject ? true : false
    this.isEditMode = false
    this.isFkEditMode = false
    this.isPkEditMode = false
    this.srcRowArray = new FormArray([])
    this.spRowArray = new FormArray([])
    this.pkData = this.conversion.getPkMapping(this.tableData)
    this.droppedColumns = []
    this.pkColumnNames = []
    this.interleaveParentName = this.getParentFromDdl()
    console.log(this.tableData)

    if (this.currentObject?.type === ObjectExplorerNodeType.Table) {
      this.setPkOrder()
      this.checkIsInterleave()

      this.interleaveObj = this.data.tableInterleaveStatus.subscribe((res) => {
        this.interleaveStatus = res
      })

      this.setSrcTableRows()
      this.setSpTableRows()
      this.setColumnsToAdd()
    } else if (this.currentObject?.type === ObjectExplorerNodeType.Index) {
      this.setIndexRows()
    }

    this.updateSpTableSuggestion()

    this.setAddPkColumnList()
    this.setPkRows()

    this.setFkRows()

    this.data.getSummary()
  }

  setSpTableRows() {
    this.spRowArray = new FormArray([])

    this.tableData.forEach((row) => {
      if (row.spOrder) {
        this.spRowArray.push(
          new FormGroup({
            srcOrder: new FormControl(row.srcOrder),
            srcColName: new FormControl(row.srcColName),
            srcDataType: new FormControl(row.srcDataType),
            srcIsPk: new FormControl(row.srcIsPk),
            srcIsNotNull: new FormControl(row.srcIsNotNull),
            spOrder: new FormControl(row.srcOrder),
            spColName: new FormControl(row.spColName),
            spDataType: new FormControl(row.spDataType),
            spIsPk: new FormControl(row.spIsPk),
            spIsNotNull: new FormControl(row.spIsNotNull),
          })
        )
      }
    })
    this.spDataSource = this.spRowArray.controls
  }

  setSrcTableRows() {
    this.srcRowArray = new FormArray([])

    this.tableData.forEach((col: IColumnTabData) => {
      if (col.spColName != '') {
        this.srcRowArray.push(
          new FormGroup({
            srcOrder: new FormControl(col.srcOrder),
            srcColName: new FormControl(col.srcColName),
            srcDataType: new FormControl(col.srcDataType),
            srcIsPk: new FormControl(col.srcIsPk),
            srcIsNotNull: new FormControl(col.srcIsNotNull),
            spOrder: new FormControl(col.spOrder),
            spColName: new FormControl(col.spColName),
            spDataType: new FormControl(col.spDataType),
            spIsPk: new FormControl(col.spIsPk),
            spIsNotNull: new FormControl(col.spIsNotNull),
          })
        )
      } else {
        this.srcRowArray.push(
          new FormGroup({
            srcOrder: new FormControl(col.srcOrder),
            srcColName: new FormControl(col.srcColName),
            srcDataType: new FormControl(col.srcDataType),
            srcIsPk: new FormControl(col.srcIsPk),
            srcIsNotNull: new FormControl(col.srcIsNotNull),
            spOrder: new FormControl(col.srcOrder),
            spColName: new FormControl(col.srcColName),
            spDataType: new FormControl(this.typeMap[col.srcDataType][0].T),
            spIsPk: new FormControl(col.srcIsPk),
            spIsNotNull: new FormControl(col.srcIsNotNull),
          })
        )
      }
    })

    this.srcDataSource = this.srcRowArray.controls
  }

  setColumnsToAdd() {
    this.tableData.forEach((col) => {
      if (!col.spColName) {
        this.srcRowArray.value.forEach((element: IColumnTabData) => {
          if (col.srcColName == element.srcColName) {
            this.droppedColumns.push(element)
          }
        })
      }
    })
  }

  toggleEdit() {
    this.currentTabIndex = 0
    if (this.isEditMode) {
      console.log(this.tableData)
      console.log(this.spRowArray.value)
      let updateData: IUpdateTable = { UpdateCols: {} }

      this.spRowArray.value.forEach((col: IColumnTabData, i: number) => {
        for (let j = 0; j < this.tableData.length; j++) {
          if (col.srcColName == this.tableData[j].srcColName) {
            let oldRow = this.tableData[j]
            updateData.UpdateCols[this.tableData[j].spColName] = {
              Add: this.tableData[j].spOrder == -1,
              Rename: oldRow.spColName !== col.spColName ? col.spColName : '',
              NotNull: col.spIsNotNull ? 'ADDED' : 'REMOVED',
              Removed: false,
              ToType: col.spDataType,
            }
            break
          }
        }
      })

      console.log(updateData)

      this.droppedColumns.forEach((col: IColumnTabData) => {
        updateData.UpdateCols[col.spColName] = {
          Add: false,
          Rename: '',
          NotNull: '',
          Removed: true,
          ToType: '',
        }
      })

      this.data.reviewTableUpdate(this.currentObject!.name, updateData).subscribe({
        next: (res: string) => {
          if (res == '') {
            this.sidenav.openSidenav()
            this.sidenav.setSidenavComponent('reviewChanges')
            this.tableUpdatePubSub.setTableUpdateDetail({
              tableName: this.currentObject!.name,
              updateDetail: updateData,
            })
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

  setColumn(columnName: string) {
    console.log(columnName)
    this.addedColumnName = columnName
  }

  addColumn() {
    let index = this.tableData.map((item) => item.srcColName).indexOf(this.addedColumnName)

    let addedRowIndex = this.droppedColumns
      .map((item) => item.srcColName)
      .indexOf(this.addedColumnName)
    this.tableData[index].spColName = this.droppedColumns[addedRowIndex].spColName
    this.tableData[index].spDataType = this.droppedColumns[addedRowIndex].spDataType
    this.tableData[index].spOrder = -1
    this.tableData[index].spIsPk = this.droppedColumns[addedRowIndex].spIsPk
    this.tableData[index].spIsNotNull = this.droppedColumns[addedRowIndex].spIsNotNull
    let ind = this.droppedColumns
      .map((col: IColumnTabData) => col.spColName)
      .indexOf(this.addedColumnName)
    if (ind > -1) {
      this.droppedColumns.splice(ind, 1)
    }
    console.log(this.tableData)
    this.setSpTableRows()
  }

  dropColumn(element: any) {
    let colName = element.get('srcColName').value
    this.spRowArray.value.forEach((col: IColumnTabData, i: number) => {
      if (col.srcColName === colName) {
        this.droppedColumns.push(col)
      }
    })
    this.dropColumnFromUI(colName)
  }

  dropColumnFromUI(colName: string) {
    this.tableData.forEach((col: IColumnTabData, i: number) => {
      if (colName == col.srcColName) {
        col.spColName = col.spColName
        col.spDataType = ''
        col.spIsNotNull = false
        col.spIsPk = false
        col.spOrder = ''
      }
    })
    this.setSpTableRows()
  }

  updateSpTableSuggestion() {
    this.isSpTableSuggesstionDisplay = []
    this.spTableSuggestion = []
    this.tableData.forEach((item: any) => {
      const srDataType = item.srcDataType
      const spDataType = item.spDataType
      let brief: string = ''
      this.typeMap[srDataType].forEach((type: any) => {
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
    arr.forEach((num: number, ind: number) => {
      this.pkData.forEach((pk: IColumnTabData) => {
        if (pk.spOrder == num) {
          pk.spOrder = ind + 1
        }
      })
    })
    if (arr.length > 0) {
      this.pkData[0].spOrder = 1
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

  setFkRows() {
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
      this.spRowArray.push(
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
    this.spDataSource = this.spRowArray.controls
  }

  toggleIndexEdit() {
    if (this.isIndexEditMode) {
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
      data: { name: this.currentObject?.name, type: 'Index' },
    })
    openDialog.afterClosed().subscribe((res: string) => {
      if (res === 'Index') {
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
    let payload: ICreateIndex[] = []
    const tableName = this.currentObject?.parent || ''
    let spIndexCount = 0
    this.indexData.forEach((idx) => {
      if (idx.spColName) spIndexCount += 1
    })
    if (spIndexCount <= 1) {
      this.dropIndex()
    } else {
      payload.push({
        Name: this.currentObject?.name || '',
        Table: this.currentObject?.parent || '',
        Unique: false,
        Keys: this.indexData
          .filter((idx, i: number) => {
            if (i === index || idx.spColName === undefined) return false
            return true
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
    }
  }

  addIndexKey() {
    let payload: ICreateIndex[] = []
    const tableName = this.currentObject?.parent || ''
    let spIndexCount = 0
    this.indexData.forEach((idx) => {
      if (idx.spColName) spIndexCount += 1
    })
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
    payload[0].Keys.push({
      Col: this.addIndexKeyForm.value.columnName,
      Desc: this.addIndexKeyForm.value.ascOrDesc === 'desc',
      Order: spIndexCount + 1,
    })
    this.data.updateIndex(tableName, payload)
    this.addIndexKeyForm.controls['columnName'].setValue('')
    this.addIndexKeyForm.controls['ascOrDesc'].setValue('')
    this.addIndexKeyForm.markAsUntouched()
  }

  restoreSpannerTable() {
    this.data
      .restoreTable(this.currentObject!.name)
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
      data: { name: this.currentObject?.name, type: 'Table' },
    })
    openDialog.afterClosed().subscribe((res: string) => {
      if (res === 'Table') {
        this.data
          .dropTable(this.currentObject!.name)
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

  addIndexColumn() {
    alert('Add column implementation is in progress.')
  }

  tabChanged(tabChangeEvent: MatTabChangeEvent): void {
    this.currentTabIndex = tabChangeEvent.index
  }
}
