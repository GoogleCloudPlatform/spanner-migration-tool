import { Component, EventEmitter, Input, OnInit, Output, SimpleChanges } from '@angular/core'
import { FormArray, FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms'
import IUpdateTable from '../../model/update-table'
import { DataService } from 'src/app/services/data/data.service'
import { MatDialog } from '@angular/material/dialog'
import { InfodialogComponent } from '../infodialog/infodialog.component'
import IColumnTabData, { IIndexData } from '../../model/edit-table'
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service'
import IFkTabData from 'src/app/model/fk-tab-data'
import { ObjectDetailNodeType, ObjectExplorerNodeType, StorageKeys } from 'src/app/app.constants'
import FlatNode from 'src/app/model/schema-object-node'
import { Subscription, take } from 'rxjs'
import { MatTabChangeEvent } from '@angular/material/tabs/tab-group'
import IConv, {
  ICreateIndex,
  IForeignKey,
  IIndexKey,
  IPkColumnDefs,
  IPrimaryKey,
} from 'src/app/model/conv'
import { ConversionService } from 'src/app/services/conversion/conversion.service'
import { DropIndexOrTableDialogComponent } from '../drop-index-or-table-dialog/drop-index-or-table-dialog.component'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'
import { TableUpdatePubSubService } from 'src/app/services/table-update-pub-sub/table-update-pub-sub.service'
import { extractSourceDbName } from 'src/app/utils/utils'

@Component({
  selector: 'app-object-detail',
  templateUrl: './object-detail.component.html',
  styleUrls: ['./object-detail.component.scss'],
})
export class ObjectDetailComponent implements OnInit {
  userAddressValidations!: FormGroup
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
  @Input() currentDatabase: string = 'spanner'
  @Input() indexData: IIndexData[] = []
  @Input() srcDbName: String = localStorage.getItem(StorageKeys.SourceDbName) as string
  @Output() updateSidebar = new EventEmitter<boolean>()
  ObjectExplorerNodeType = ObjectExplorerNodeType
  conv: IConv = {} as IConv
  interleaveObj!: Subscription
  interleaveStatus: any
  interleaveParentName: string | null = null
  localTableData: IColumnTabData[] = []
  localIndexData: IIndexData[] = []
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
    this.currentDatabase = changes['currentDatabase']?.currentValue || this.currentDatabase
    this.currentTabIndex = this.currentObject?.type === ObjectExplorerNodeType.Table ? 0 : -1
    this.isObjectSelected = this.currentObject ? true : false
    this.pkData = this.conversion.getPkMapping(this.tableData)
    this.interleaveParentName = this.getInterleaveParentFromConv()

    this.isEditMode = false
    this.isFkEditMode = false
    this.isIndexEditMode = false
    this.isPkEditMode = false
    this.srcRowArray = new FormArray([])
    this.spRowArray = new FormArray([])
    this.droppedColumns = []
    this.pkColumnNames = []
    this.interleaveParentName = this.getInterleaveParentFromConv()

    this.localTableData = JSON.parse(JSON.stringify(this.tableData))
    this.localIndexData = JSON.parse(JSON.stringify(this.indexData))

    if (this.currentObject?.type === ObjectExplorerNodeType.Table) {
      this.checkIsInterleave()

      this.interleaveObj = this.data.tableInterleaveStatus.subscribe((res) => {
        this.interleaveStatus = res
      })

      this.setSrcTableRows()
      this.setSpTableRows()
      this.setColumnsToAdd()
      this.setAddPkColumnList()
      this.setPkOrder()
      this.setPkRows()
      this.setFkRows()
      this.updateSpTableSuggestion()
    } else if (this.currentObject?.type === ObjectExplorerNodeType.Index) {
      this.checkIsInterleave()
      this.indexOrderValidation()
      this.setIndexRows()
    }

    this.data.getSummary()
  }

  setSpTableRows() {
    this.spRowArray = new FormArray([])
    this.localTableData.forEach((row) => {
      if (row.spOrder) {
        this.spRowArray.push(
          new FormGroup({
            srcOrder: new FormControl(row.srcOrder),
            srcColName: new FormControl(row.srcColName),
            srcDataType: new FormControl(row.srcDataType),
            srcIsPk: new FormControl(row.srcIsPk),
            srcIsNotNull: new FormControl(row.srcIsNotNull),
            spOrder: new FormControl(row.srcOrder),
            spColName: new FormControl(row.spColName, [
              Validators.required,
              Validators.pattern('^[a-zA-Z]([a-zA-Z0-9/_]*[a-zA-Z0-9])?'),
            ]),
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

    this.localTableData.forEach((col: IColumnTabData) => {
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
    this.localTableData.forEach((col) => {
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
      this.localTableData = JSON.parse(JSON.stringify(this.tableData))
      this.setSpTableRows()
      this.isEditMode = false
    } else {
      this.isEditMode = true
    }
  }
  saveColumnTable() {
    this.isEditMode = false
    let updateData: IUpdateTable = { UpdateCols: {} }

    this.spRowArray.value.forEach((col: IColumnTabData, i: number) => {
      for (let j = 0; j < this.tableData.length; j++) {
        if (col.srcColName == this.tableData[j].srcColName) {
          let oldRow = this.tableData[j]
          let columnName =
            this.tableData[j].spColName == ''
              ? this.tableData[j].srcColName
              : this.tableData[j].spColName
          updateData.UpdateCols[columnName] = {
            Add: this.tableData[j].spColName == '',
            Rename: oldRow.spColName !== col.spColName ? col.spColName : '',
            NotNull: col.spIsNotNull ? 'ADDED' : 'REMOVED',
            Removed: false,
            ToType: col.spDataType,
          }
          break
        }
      }
    })

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
          this.isEditMode = true
        } else {
          this.dialog.open(InfodialogComponent, {
            data: { message: res, type: 'error' },
            maxWidth: '500px',
          })
          this.isEditMode = true
        }
      },
    })
  }

  setColumn(columnName: string) {
    this.addedColumnName = columnName
  }

  addColumn() {
    let index = this.localTableData.map((item) => item.srcColName).indexOf(this.addedColumnName)

    let addedRowIndex = this.droppedColumns
      .map((item) => item.srcColName)
      .indexOf(this.addedColumnName)
    this.localTableData[index].spColName = this.droppedColumns[addedRowIndex].spColName
    this.localTableData[index].spDataType = this.droppedColumns[addedRowIndex].spDataType
    this.localTableData[index].spOrder = -1
    this.localTableData[index].spIsPk = this.droppedColumns[addedRowIndex].spIsPk
    this.localTableData[index].spIsNotNull = this.droppedColumns[addedRowIndex].spIsNotNull
    let ind = this.droppedColumns
      .map((col: IColumnTabData) => col.spColName)
      .indexOf(this.addedColumnName)
    if (ind > -1) {
      this.droppedColumns.splice(ind, 1)
    }
    this.setSpTableRows()
  }

  dropColumn(element: any) {
    let colName = element.get('srcColName').value
    let spColName = this.conv.ToSpanner[this.currentObject!.name].Cols[colName]
    console.log(spColName, 'spColName')
    let associatedIndexes = this.getAssociatedIndexs(spColName)
    if (this.checkIfPkColumn(spColName) || associatedIndexes.length != 0) {
      let pkWarning: string = ''
      let indexWaring: string = ''
      let connectingString: string = ''
      if (this.checkIfPkColumn(spColName)) {
        pkWarning = ` Primary key`
      }
      if (associatedIndexes.length != 0) {
        indexWaring = ` Index ${associatedIndexes}`
      }
      if (pkWarning != '' && indexWaring != '') {
        connectingString = ` and`
      }
      this.dialog.open(InfodialogComponent, {
        data: {
          message: `Column ${spColName} is a part of${pkWarning}${connectingString}${indexWaring}. Remove the dependencies from respective tabs before dropping the Column. `,
          type: 'error',
        },
        maxWidth: '500px',
      })
    } else {
      this.spRowArray.value.forEach((col: IColumnTabData, i: number) => {
        if (col.srcColName === colName) {
          this.droppedColumns.push(col)
        }
      })
      this.dropColumnFromUI(colName)
    }
  }

  checkIfPkColumn(colName: string) {
    let isPkColumn = false
    if (
      this.conv.SpSchema[this.currentObject!.name].Pks != null &&
      this.conv.SpSchema[this.currentObject!.name].Pks.map((pk: IIndexKey) => pk.Col).includes(
        colName
      )
    ) {
      isPkColumn = true
    }
    return isPkColumn
  }

  getAssociatedIndexs(colName: string) {
    let indexes: string[] = []
    if (this.conv.SpSchema[this.currentObject!.name].Indexes != null) {
      this.conv.SpSchema[this.currentObject!.name].Indexes.forEach((ind: ICreateIndex) => {
        if (ind.Keys.map((key) => key.Col).includes(colName)) {
          indexes.push(ind.Name)
        }
      })
    }
    return indexes
  }

  dropColumnFromUI(colName: string) {
    this.localTableData.forEach((col: IColumnTabData, i: number) => {
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
    this.localTableData.forEach((item: any) => {
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
    const srDataType = this.localTableData[index].srcDataType
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
          spOrder: new FormControl(spArr[i].spOrder, [
            Validators.required,
            Validators.pattern('^[1-9][0-9]*$'),
          ]),
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
    let index = this.localTableData.map((item) => item.spColName).indexOf(this.addedPkColumnName)
    let newColumnOrder = 1
    this.localTableData[index].spIsPk = true
    this.pkData = []
    this.pkData = this.conversion.getPkMapping(this.localTableData)
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
    for (let i = 0; i < this.localTableData.length; i++) {
      if (
        !currentPkColumns.includes(this.localTableData[i].spColName) &&
        this.localTableData[i].spColName !== ''
      )
        this.pkColumnNames.push(this.localTableData[i].spColName)
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
          pk.spOrder = this.conv.SpSchema[this.currentObject!.name].Pks[index]?.Order
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
    let arr = this.pkData
      .filter((column: IColumnTabData) => {
        return column.spIsPk
      })
      .map((item) => Number(item.spOrder))
    arr.sort((a, b) => a - b)
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
      this.localTableData = JSON.parse(JSON.stringify(this.tableData))
      this.pkData = this.conversion.getPkMapping(this.tableData)
      this.setAddPkColumnList()
      this.setPkOrder()
      this.setPkRows()
      this.isPkEditMode = false
    } else {
      this.isPkEditMode = true
    }
  }

  savePk() {
    this.pkArray.value.forEach((pk: IColumnTabData) => {
      for (let i = 0; i < this.pkData.length; i++) {
        if (pk.spColName == this.pkData[i].spColName) {
          this.pkData[i].spOrder = Number(pk.spOrder)
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
    } else {
      let interleaveTable = this.tableInterleaveWith(this.currentObject?.name!)
      if (interleaveTable != '' && this.isPKFirstOrderModified(this.currentObject?.name!)) {
        const dialogRef = this.dialog.open(InfodialogComponent, {
          data: {
            message:
              'Proceeding the update will remove interleaving between ' +
              this.currentObject?.name +
              ' and ' +
              interleaveTable +
              ' tables.',
            title: 'Confirm Update',
            type: 'warning',
          },
          maxWidth: '500px',
        })
        dialogRef.afterClosed().subscribe((dialogResult) => {
          if (dialogResult) {
            let interleavedChildId: string =
              this.conv.SpSchema[this.currentObject!.name].Parent != ''
                ? this.currentObject!.id
                : this.conv.SpSchema[interleaveTable].Id
            this.data
              .removeInterleave(interleavedChildId)
              .pipe(take(1))
              .subscribe((res: string) => {
                this.updatePk()
              })
          }
        })
      } else {
        this.updatePk()
      }
    }
  }

  updatePk() {
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
  }

  dropPk(element: any) {
    let index = this.localTableData.map((item) => item.spColName).indexOf(element.value.spColName)
    let removedOrder = element.value.spOrder
    this.localTableData[index].spIsPk = false
    this.pkData = []
    this.pkData = this.conversion.getPkMapping(this.localTableData)
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
    var srcArr = new Array()
    var spArr = new Array()
    this.fkData.forEach((fk) => {
      srcArr.push({
        srcName: fk.srcName,
        srcColumns: fk.srcColumns,
        srcRefTable: fk.srcReferTable,
        srcRefColumns: fk.srcReferColumns,
        Id: fk.Id,
      })
      if (fk.spName != '') {
        spArr.push({
          spName: fk.spName,
          spColumns: fk.spColumns,
          spRefTable: fk.spReferTable,
          spRefColumns: fk.spReferColumns,
          Id: fk.Id,
        })
      }
    })
    for (let i = 0; i < Math.min(srcArr.length, spArr.length); i++) {
      this.fkArray.push(
        new FormGroup({
          spName: new FormControl(spArr[i].spName, [
            Validators.required,
            Validators.pattern('^[a-zA-Z]([a-zA-Z0-9/_]*[a-zA-Z0-9])?'),
          ]),
          srcName: new FormControl(srcArr[i].srcName),
          spColumns: new FormControl(spArr[i].spColumns),
          srcColumns: new FormControl(srcArr[i].srcColumns),
          spReferTable: new FormControl(spArr[i].spRefTable),
          srcReferTable: new FormControl(srcArr[i].srcRefTable),
          spReferColumns: new FormControl(spArr[i].spRefColumns),
          srcReferColumns: new FormControl(srcArr[i].srcRefColumns),
          Id: new FormControl(spArr[i].Id),
        })
      )
    }

    if (srcArr.length > Math.min(srcArr.length, spArr.length))
      for (let i = Math.min(srcArr.length, spArr.length); i < srcArr.length; i++) {
        this.fkArray.push(
          new FormGroup({
            spName: new FormControl('', [
              Validators.required,
              Validators.pattern('^[a-zA-Z]([a-zA-Z0-9/_]*[a-zA-Z0-9])?'),
            ]),
            srcName: new FormControl(srcArr[i].srcName),
            spColumns: new FormControl([]),
            srcColumns: new FormControl(srcArr[i].srcColumns),
            spReferTable: new FormControl(''),
            srcReferTable: new FormControl(srcArr[i].srcRefTable),
            spReferColumns: new FormControl([]),
            srcReferColumns: new FormControl(srcArr[i].srcRefColumns),
            Id: new FormControl(srcArr[i].Id),
          })
        )
      }
    this.fkDataSource = this.fkArray.controls
  }

  toggleFkEdit() {
    this.currentTabIndex = 2
    if (this.isFkEditMode) {
      this.setFkRows()
      this.isFkEditMode = false
    } else {
      this.currentTabIndex = 2
      this.isFkEditMode = true
    }
  }

  saveFk() {
    let spFkArr: IForeignKey[] = []
    this.fkArray.value.forEach((fk: IFkTabData) => {
      spFkArr.push({
        Name: fk.spName,
        Columns: fk.spColumns,
        ReferTable: fk.spReferTable,
        ReferColumns: fk.spReferColumns,
        Id: fk.Id,
      })
    })

    this.data.updateFkNames(this.currentObject!.name, spFkArr).subscribe({
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
  }

  dropFk(element: any) {
    this.fkData.forEach((fk) => {
      if (fk.spName == element.get('spName').value) {
        fk.spName = ''
        fk.spColumns = []
        fk.spReferTable = ''
        fk.spReferColumns = []
      }
    })
    this.setFkRows()
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

  removeInterleave() {
    let tableId = this.currentObject!.id
    this.data
      .removeInterleave(tableId)
      .pipe(take(1))
      .subscribe((res: string) => {
        if (res === '') {
          this.snackbar.openSnackBar(
            'Interleave removed and foreign key restored successfully',
            'Close',
            5
          )
        }
      })
  }

  checkIsInterleave() {
    if (!this.currentObject?.isDeleted && this.currentObject?.isSpannerNode) {
      this.data.getInterleaveConversionForATable(this.currentObject!.name)
    }
  }

  setInterleave() {
    this.data.setInterleave(this.currentObject!.name)
  }

  getInterleaveParentFromConv() {
    return this.currentObject?.type === ObjectExplorerNodeType.Table &&
      this.currentObject.isSpannerNode &&
      !this.currentObject.isDeleted &&
      this.conv.SpSchema[this.currentObject.name].Parent != ''
      ? this.conv.SpSchema[this.currentObject.name].Parent
      : null
  }

  setIndexRows() {
    this.spRowArray = new FormArray([])
    const addedIndexColumns: string[] = this.localIndexData
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
    this.localIndexData.forEach((row: IIndexData) => {
      this.spRowArray.push(
        new FormGroup({
          srcOrder: new FormControl(row.srcOrder),
          srcColName: new FormControl(row.srcColName),
          srcDesc: new FormControl(row.srcDesc),
          spOrder: new FormControl(row.spOrder),
          spColName: new FormControl(row.spColName, [
            Validators.required,
            Validators.pattern('^[a-zA-Z]([a-zA-Z0-9/_]*[a-zA-Z0-9])?'),
          ]),
          spDesc: new FormControl(row.spDesc),
        })
      )
    })
    this.spDataSource = this.spRowArray.controls
  }

  setIndexOrder() {
    this.spRowArray.value.forEach((idx: IIndexData) => {
      for (let i = 0; i < this.localIndexData.length; i++) {
        if (idx.spColName != '' && idx.spColName == this.localIndexData[i].spColName) {
          this.localIndexData[i].spOrder = idx.spOrder
          break
        }
      }
    })
    this.indexOrderValidation()
  }

  indexOrderValidation() {
    let arr = this.localIndexData
      .filter((idx) => {
        return idx.spColName != ''
      })
      .map((item) => Number(item.spOrder))
    arr.sort((a, b) => a - b)
    if (arr[arr.length - 1] > arr.length) {
      arr.forEach((num: number, i: number) => {
        this.localIndexData.forEach((ind: IIndexData) => {
          if (ind.spColName != '' && ind.spOrder == num) {
            ind.spOrder = i + 1
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
      this.localIndexData.forEach((idx: any) => {
        if (idx.spOrder < missingOrder) {
          idx.spOrder = Number(idx.spOrder) + 1
        }
      })
    }
  }

  toggleIndexEdit() {
    if (this.isIndexEditMode) {
      this.localIndexData = JSON.parse(JSON.stringify(this.indexData))
      this.setIndexRows()
      this.isIndexEditMode = false
    } else {
      this.isIndexEditMode = true
    }
  }

  saveIndex() {
    let payload: ICreateIndex[] = []
    const tableName = this.currentObject?.parent || ''
    this.setIndexOrder()
    payload.push({
      Name: this.currentObject?.name || '',
      Table: this.currentObject?.parent || '',
      Unique: false,
      Keys: this.localIndexData
        .filter((idx: any) => {
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

    this.data.updateIndex(tableName, payload).subscribe({
      next: (res: string) => {
        if (res == '') {
          this.isEditMode = false
        } else {
          this.dialog.open(InfodialogComponent, {
            data: { message: res, type: 'error' },
            maxWidth: '500px',
          })
          this.isIndexEditMode = true
        }
      },
    })
    this.addIndexKeyForm.controls['columnName'].setValue('')
    this.addIndexKeyForm.controls['ascOrDesc'].setValue('')
    this.addIndexKeyForm.markAsUntouched()
    this.data.getSummary()
    this.isIndexEditMode = false
  }

  dropIndex() {
    let openDialog = this.dialog.open(DropIndexOrTableDialogComponent, {
      width: '35vw',
      minWidth: '450px',
      maxWidth: '600px',
      data: { name: this.currentObject?.name, type: ObjectDetailNodeType.Index },
    })
    openDialog.afterClosed().subscribe((res: string) => {
      if (res === ObjectDetailNodeType.Index) {
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

  restoreIndex() {
    let tableId = this.currentObject!.parentId
    let indexId = this.currentObject!.id
    this.data
      .restoreIndex(tableId, indexId)
      .pipe(take(1))
      .subscribe((res: string) => {
        if (res === '') {
          this.isObjectSelected = false
        }
      })
    this.currentObject = null
  }

  dropIndexKey(index: number) {
    if (this.localIndexData[index].srcColName) {
      this.localIndexData[index].spColName = ''
      this.localIndexData[index].spDesc = ''
      this.localIndexData[index].spOrder = ''
    } else {
      this.localIndexData.splice(index, 1)
    }
    this.setIndexRows()
  }

  addIndexKey() {
    let spIndexCount = 0
    this.localIndexData.forEach((idx) => {
      if (idx.spColName) spIndexCount += 1
    })
    this.localIndexData.push({
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
        this.data.getConversionRate()
        this.data.getDdl()
      })

    this.currentObject = null
  }

  dropTable() {
    let openDialog = this.dialog.open(DropIndexOrTableDialogComponent, {
      width: '35vw',
      minWidth: '450px',
      maxWidth: '600px',
      data: { name: this.currentObject?.name, type: ObjectDetailNodeType.Table },
    })
    openDialog.afterClosed().subscribe((res: string) => {
      if (res === ObjectDetailNodeType.Table) {
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

  tableInterleaveWith(table: string): string {
    if (this.conv.SpSchema[table].Parent != '') {
      return this.conv.SpSchema[table].Parent
    }
    let interleaveTable = ''
    Object.keys(this.conv.SpSchema).forEach((tableName: string) => {
      if (
        this.conv.SpSchema[tableName].Parent != '' &&
        this.conv.SpSchema[tableName].Parent == table
      ) {
        interleaveTable = tableName
      }
    })

    return interleaveTable
  }

  isPKFirstOrderModified(table: string): boolean {
    let firstOrderPk = this.conv.SpSchema[table].Pks.filter((pk: IIndexKey) => {
      if (pk.Order == 1) return true
      return false
    })[0]
    if (this.pkObj.Columns.length < 1) return true
    let updatedFirstOrderPk = this.pkObj.Columns.filter((pk: IPkColumnDefs) => {
      if (pk.Order == 1) return true
      return false
    })[0]
    if (firstOrderPk.Col != updatedFirstOrderPk.ColName) return true
    return false
  }
}
