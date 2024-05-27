import { Component, EventEmitter, Input, OnInit, Output, SimpleChanges } from '@angular/core'
import { FormArray, FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms'
import IUpdateTable from '../../model/update-table'
import { DataService } from 'src/app/services/data/data.service'
import { MatDialog } from '@angular/material/dialog'
import { InfodialogComponent } from '../infodialog/infodialog.component'
import IColumnTabData, { AutoGen, IIndexData } from '../../model/edit-table'
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service'
import IFkTabData from 'src/app/model/fk-tab-data'
import { ColLength, Dialect, ObjectDetailNodeType, ObjectExplorerNodeType, SourceDbNames, StorageKeys } from 'src/app/app.constants'
import FlatNode from 'src/app/model/schema-object-node'
import { Subscription, take } from 'rxjs'
import { MatTabChangeEvent } from '@angular/material/tabs/'
import IConv, {
  ICreateIndex,
  IForeignKey,
  IIndexKey,
  IPrimaryKey,
} from 'src/app/model/conv'
import { ConversionService } from 'src/app/services/conversion/conversion.service'
import { DropIndexOrTableDialogComponent } from '../drop-index-or-table-dialog/drop-index-or-table-dialog.component'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'
import { TableUpdatePubSubService } from 'src/app/services/table-update-pub-sub/table-update-pub-sub.service'
import { AddNewColumnComponent } from '../add-new-column/add-new-column.component'
import { GroupedAutoGens, processAutoGens } from 'src/app/utils/utils'

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
    private tableUpdatePubSub: TableUpdatePubSubService,
    private fb: FormBuilder,
  ) { }

  @Input() currentObject: FlatNode | null = null
  @Input() typeMap: any = {}
  @Input() defaultTypeMap: any = {}
  @Input() autoGenMap: any = {}
  @Input() ddlStmts: any = {}
  @Input() fkData: IFkTabData[] = []
  @Input() tableData: IColumnTabData[] = []
  @Input() currentDatabase: string = 'spanner'
  @Input() indexData: IIndexData[] = []
  @Input() srcDbName: string = localStorage.getItem(StorageKeys.SourceDbName) as string
  @Output() updateSidebar = new EventEmitter<boolean>()
  ObjectExplorerNodeType = ObjectExplorerNodeType
  conv: IConv = {} as IConv
  interleaveObj!: Subscription
  interleaveStatus: any
  interleaveParentName: string | null = null
  localTableData: IColumnTabData[] = []
  localIndexData: IIndexData[] = []
  isMiddleColumnCollapse: boolean = false
  isPostgreSQLDialect: boolean = false
  processedAutoGenMap: GroupedAutoGens = {};
  autoGenSupportedDbs: string[] = ['MySQL']
  autGenSupported: boolean = false
  ngOnInit(): void {
    this.data.conv.subscribe({
      next: (res: IConv) => {
        this.conv = res
        this.isPostgreSQLDialect = this.conv.SpDialect === Dialect.PostgreSQLDialect
      },
    })
    this.autGenSupported = this.autoGenSupportedDbs.includes(this.srcDbName)
  }

  srcDisplayedColumns = ['srcOrder', 'srcColName', 'srcDataType', 'srcColMaxLength', 'srcIsPk', 'srcIsNotNull']

  spDisplayedColumns = ['spColName', 'spDataType', 'spColMaxLength', 'spIsPk', 'spIsNotNull', 'dropButton']
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
  srcRowArray: FormArray = this.fb.array([])
  spRowArray: FormArray = this.fb.array([])
  pkArray: FormArray = this.fb.array([])
  fkArray: FormArray = this.fb.array([])
  isSpTableSuggesstionDisplay: boolean[] = []
  spTableSuggestion: string[] = []
  currentTabIndex: number = 0
  addedColumnName: string = ''
  droppedColumns: IColumnTabData[] = []
  droppedSourceColumns: string[] = []
  pkColumnNames: string[] = []
  indexColumnNames: string[] = []
  shardIdCol: string = ''
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
  dataTypesWithColLen: string[] = ColLength.DataTypes
  spColspan : number = 6

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
    this.srcRowArray = this.fb.array([])
    this.spRowArray = this.fb.array([])
    this.droppedColumns = []
    this.droppedSourceColumns = []
    this.pkColumnNames = []
    this.interleaveParentName = this.getInterleaveParentFromConv()

    this.localTableData = JSON.parse(JSON.stringify(this.tableData))
    this.localIndexData = JSON.parse(JSON.stringify(this.indexData))

    if (this.srcDbName == SourceDbNames.MySQL && !this.spDisplayedColumns.includes("spAutoGen")) {
      this.spDisplayedColumns.splice(2, 0, "spAutoGen");
      this.spColspan++;
    }

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
      this.setShardIdColumn()
      this.processedAutoGenMap = processAutoGens(this.autoGenMap)
    } else if (this.currentObject?.type === ObjectExplorerNodeType.Index) {
      this.indexOrderValidation()
      this.setIndexRows()
    }

    this.data.getSummary()
  }

  setSpTableRows() {
    this.spRowArray = this.fb.array([])
    this.localTableData.forEach((row) => {
      if (row.spOrder) {
        let fb = new FormGroup({
          srcOrder: new FormControl(row.srcOrder),
          srcColName: new FormControl(row.srcColName),
          srcDataType: new FormControl(row.srcDataType),
          srcIsPk: new FormControl(row.srcIsPk),
          srcIsNotNull: new FormControl(row.srcIsNotNull),
          srcColMaxLength: new FormControl(row.srcColMaxLength),
          spOrder: new FormControl(row.srcOrder),
          spColName: new FormControl(row.spColName, [
            Validators.required,
            Validators.pattern('^[a-zA-Z]([a-zA-Z0-9/_]*[a-zA-Z0-9])?'),
          ]),
          spDataType: new FormControl(row.spDataType),
          spIsPk: new FormControl(row.spIsPk),
          spIsNotNull: new FormControl(row.spIsNotNull), spId: new FormControl(row.spId),
          srcId: new FormControl(row.srcId),
          spColMaxLength: new FormControl(row.spColMaxLength, [
            Validators.required]),
          spAutoGen: new FormControl(row.spAutoGen),
        })
        if (this.dataTypesWithColLen.indexOf(row.spDataType.toString()) > -1) {
          fb.get('spColMaxLength')?.setValidators([Validators.required, Validators.pattern('([1-9][0-9]*|MAX)')])
          if (row.spColMaxLength === undefined) {
            fb.get('spColMaxLength')?.setValue('MAX')
          }
          else if (row.spColMaxLength !== 'MAX') {
            if ((row.spDataType === 'STRING' || row.spDataType === 'VARCHAR') && typeof row.spColMaxLength === "number" && row.spColMaxLength > ColLength.StringMaxLength) {
              fb.get('spColMaxLength')?.setValue('MAX')
            } else if (row.spDataType === 'BYTES' && typeof row.spColMaxLength === "number" && row.spColMaxLength > ColLength.ByteMaxLength) {
              fb.get('spColMaxLength')?.setValue('MAX')
            }
          }
        } else {
          fb.controls['spColMaxLength'].clearValidators()
        }
        fb.controls['spColMaxLength'].updateValueAndValidity()
        this.spRowArray.push(
          fb
        )
      }
    })
    this.spDataSource = this.spRowArray.controls
  }

  setSrcTableRows() {
    this.srcRowArray = this.fb.array([])

    this.localTableData.forEach((col: IColumnTabData) => {
      if (col.spColName != '') {
        this.srcRowArray.push(
          new FormGroup({
            srcOrder: new FormControl(col.srcOrder),
            srcColName: new FormControl(col.srcColName),
            srcDataType: new FormControl(col.srcDataType),
            srcIsPk: new FormControl(col.srcIsPk),
            srcIsNotNull: new FormControl(col.srcIsNotNull),
            srcColMaxLength: new FormControl(col.srcColMaxLength),
            spOrder: new FormControl(col.spOrder),
            spColName: new FormControl(col.spColName),
            spDataType: new FormControl(col.spDataType),
            spIsPk: new FormControl(col.spIsPk),
            spIsNotNull: new FormControl(col.spIsNotNull),
            spId: new FormControl(col.spId),
            srcId: new FormControl(col.srcId),
            spColMaxLength: new FormControl(col.spColMaxLength),
            spAutoGen: new FormControl(col.spAutoGen),
          })
        )
      } else {
        let droppedColumnSpDataType = ''
        let droppedColumnSpMaxLength = col.srcColMaxLength
        if (this.defaultTypeMap[col.srcDataType] === undefined) {
          droppedColumnSpDataType = 'STRING'
          droppedColumnSpMaxLength = 'MAX'
        } else {
          droppedColumnSpDataType = this.defaultTypeMap[col.srcDataType].Name
        }
        this.srcRowArray.push(
          new FormGroup({
            srcOrder: new FormControl(col.srcOrder),
            srcColName: new FormControl(col.srcColName),
            srcDataType: new FormControl(col.srcDataType),
            srcIsPk: new FormControl(col.srcIsPk),
            srcIsNotNull: new FormControl(col.srcIsNotNull),
            srcColMaxLength: new FormControl(col.srcColMaxLength),
            spOrder: new FormControl(col.srcOrder),
            spColName: new FormControl(col.srcColName),
            spDataType: new FormControl(
              droppedColumnSpDataType
            ),
            spIsPk: new FormControl(col.srcIsPk),
            spIsNotNull: new FormControl(col.srcIsNotNull),
            spColMaxLength: new FormControl(droppedColumnSpMaxLength),
            spAutoGen: new FormControl(col.spAutoGen)
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
          if (col.srcColName == element.srcColName && element.srcColName != '') {
            this.droppedColumns.push(element)
            this.droppedSourceColumns.push(element.srcColName)
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
    let pgSQLToStandardTypeTypemap: Map<String, String>;
    this.conversion.pgSQLToStandardTypeTypeMap.subscribe((typemap) => {
      pgSQLToStandardTypeTypemap = typemap
    })
    this.spRowArray.value.forEach((col: IColumnTabData, i: number) => {
      for (let j = 0; j < this.tableData.length; j++) {
        let oldRow = this.tableData[j]
        let standardDataType = pgSQLToStandardTypeTypemap.get(col.spDataType)
        if (col.spColMaxLength !== undefined && col.spColMaxLength !== 'MAX') {
          if ((col.spDataType === 'STRING' || col.spDataType === 'VARCHAR') && typeof col.spColMaxLength === "number" && col.spColMaxLength > ColLength.StringMaxLength) {
            col.spColMaxLength = 'MAX'
          } else if (col.spDataType === 'BYTES' && typeof col.spColMaxLength === "number" && col.spColMaxLength > ColLength.ByteMaxLength) {
            col.spColMaxLength = 'MAX'
          }
        }
        if (typeof (col.spColMaxLength) === 'number') {
          col.spColMaxLength = col.spColMaxLength.toString()
        }
        if (col.spDataType != 'STRING' && col.spDataType != 'BYTES' && col.spDataType != 'VARCHAR') {
          col.spColMaxLength = ""
        }
        if (col.srcId == this.tableData[j].srcId && this.tableData[j].srcId != '') {
          updateData.UpdateCols[this.tableData[j].srcId] = {
            Add: this.tableData[j].spId == '',
            Rename: oldRow.spColName !== col.spColName ? col.spColName : '',
            NotNull: col.spIsNotNull ? 'ADDED' : 'REMOVED',
            Removed: false,
            ToType: (this.conv.SpDialect === Dialect.PostgreSQLDialect) ? (standardDataType === undefined ? col.spDataType : standardDataType) : col.spDataType,
            MaxColLength: col.spColMaxLength,
            AutoGen: col.spAutoGen
          }
          break
        }
        else if (col.spId == this.tableData[j].spId) {
          updateData.UpdateCols[this.tableData[j].spId] = {
            Add: this.tableData[j].spId == '',
            Rename: oldRow.spColName !== col.spColName ? col.spColName : '',
            NotNull: col.spIsNotNull ? 'ADDED' : 'REMOVED',
            Removed: false,
            ToType: (this.conv.SpDialect === Dialect.PostgreSQLDialect) ? (standardDataType === undefined ? col.spDataType : standardDataType) : col.spDataType,
            MaxColLength: col.spColMaxLength,
            AutoGen: col.spAutoGen
          }
        }
      }
    })

    this.droppedColumns.forEach((col: IColumnTabData) => {
      updateData.UpdateCols[col.spId] = {
        Add: false,
        Rename: '',
        NotNull: '',
        Removed: true,
        ToType: '',
        MaxColLength: '',
        AutoGen: {
          Name : '',
          GenerationType : ''
        }
      }
    })

    this.data.reviewTableUpdate(this.currentObject!.id, updateData).subscribe({
      next: (res: string) => {
        if (res == '') {
          this.sidenav.openSidenav()
          this.sidenav.setSidenavComponent('reviewChanges')
          this.tableUpdatePubSub.setTableUpdateDetail({
            tableName: this.currentObject!.name,
            tableId: this.currentObject!.id,
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

  addNewColumn() {
    this.dialog.open(AddNewColumnComponent, {
      width: '30vw',
      minWidth: '400px',
      maxWidth: '500px',
      data: {
        dialect: this.conv.SpDialect,
        tableId: this.currentObject?.id,
      }
    })
  }

  setColumn(columnName: string) {
    this.addedColumnName = columnName
  }

  restoreColumn() {
    let index = this.tableData.map((item) => item.srcColName).indexOf(this.addedColumnName)

    let addedRowIndex = this.droppedColumns
      .map((item) => item.srcColName)
      .indexOf(this.addedColumnName)
    this.localTableData[index].spColName = this.droppedColumns[addedRowIndex].spColName
    this.localTableData[index].spDataType = this.droppedColumns[addedRowIndex].spDataType
    this.localTableData[index].spOrder = -1
    this.localTableData[index].spIsPk = this.droppedColumns[addedRowIndex].spIsPk
    this.localTableData[index].spIsNotNull = this.droppedColumns[addedRowIndex].spIsNotNull
    this.localTableData[index].spColMaxLength = this.droppedColumns[addedRowIndex].spColMaxLength
    this.localTableData[index].spAutoGen = this.droppedColumns[addedRowIndex].spAutoGen
    let ind = this.droppedColumns
      .map((col: IColumnTabData) => col.spColName)
      .indexOf(this.addedColumnName)
    if (ind > -1) {
      this.droppedColumns.splice(ind, 1)
      if (this.droppedSourceColumns.indexOf(this.addedColumnName) > -1) {
        this.droppedSourceColumns.splice(this.droppedSourceColumns.indexOf(this.addedColumnName), 1)
      }
    }
    this.setSpTableRows()
  }

  dropColumn(element: any) {
    let srcColName = element.get('srcColName').value
    let srcColId = element.get('srcId').value
    let spColId = element.get('spId').value
    let colId = srcColId != '' ? srcColId : spColId
    let spColName = element.get('spColName').value

    let associatedIndexes = this.getAssociatedIndexs(colId)
    if (this.checkIfPkColumn(colId) || associatedIndexes.length != 0) {
      let pkWarning: string = ''
      let indexWaring: string = ''
      let connectingString: string = ''
      if (this.checkIfPkColumn(colId)) {
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
        if (col.spId === spColId) {
          this.droppedColumns.push(col)
        }
      })
      this.dropColumnFromUI(spColId)
      if (srcColName !== '') {
        this.droppedSourceColumns.push(srcColName)
      }
    }
  }

  checkIfPkColumn(colId: string) {
    let isPkColumn = false
    if (
      this.conv.SpSchema[this.currentObject!.id].PrimaryKeys != null &&
      this.conv.SpSchema[this.currentObject!.id].PrimaryKeys.map(
        (pk: IIndexKey) => pk.ColId
      ).includes(colId)
    ) {
      isPkColumn = true
    }
    return isPkColumn
  }

  setShardIdColumn() {
    if (this.conv.SpSchema[this.currentObject!.id] !== undefined) {
      this.shardIdCol = this.conv.SpSchema[this.currentObject!.id].ShardIdColumn
    }

  }

  getAssociatedIndexs(colId: string) {
    let indexes: string[] = []
    if (this.conv.SpSchema[this.currentObject!.id].Indexes != null) {
      this.conv.SpSchema[this.currentObject!.id].Indexes.forEach((ind: ICreateIndex) => {
        if (ind.Keys.map((key) => key.ColId).includes(colId)) {
          indexes.push(ind.Name)
        }
      })
    }
    return indexes
  }

  dropColumnFromUI(spColId: string) {
    this.localTableData.forEach((col: IColumnTabData, i: number) => {
      if (col.spId == spColId) {
        col.spColName = ''
        col.spDataType = ''
        col.spIsNotNull = false
        col.spIsPk = false
        col.spOrder = ''
        col.spColMaxLength = ''
        col.spAutoGen = {
          Name : '',
          GenerationType : ''
        }
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
        if (spDataType == type.DiplayT) brief = type.Brief
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

  compareAutoGen(t1: any, t2: any): boolean {
    return t1 && t2 ? t1.Name === t2.Name && t1.GenerationType === t2.GenerationType : t1 === t2;
  }
 
  setPkRows() {
    this.pkArray = this.fb.array([])
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
          srcId: row.srcId,
        })
      }
      if (row.spIsPk) {
        spArr.push({
          spColName: row.spColName,
          spDataType: row.spDataType,
          spIsNotNull: row.spIsNotNull,
          spIsPk: row.spIsPk,
          spOrder: row.spOrder,
          spId: row.spId,
          spAutoGen: row.spAutoGen
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
          spId: new FormControl(spArr[i].spId),
          spAutoGen: new FormControl(spArr[i].spAutoGen),
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
            srcId: new FormControl(srcArr[i].srcId),
            spOrder: new FormControl(''),
            spColName: new FormControl(''),
            spDataType: new FormControl(''),
            spIsPk: new FormControl(false),
            spIsNotNull: new FormControl(false),
            spId: new FormControl(''),
            spAutoGen: new FormControl(spArr[i].spAutoGen),
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
            srcId: new FormControl(''),
            spOrder: new FormControl(spArr[i].spOrder),
            spColName: new FormControl(spArr[i].spColName),
            spDataType: new FormControl(spArr[i].spDataType),
            spIsPk: new FormControl(spArr[i].spIsPk),
            spIsNotNull: new FormControl(spArr[i].spIsNotNull),
            spId: new FormControl(spArr[i].spId),
            spAutoGenGen: new FormControl(spArr[i].spAutoGen)
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
    let toAddCol = this.localTableData[index]
    let newColumnOrder = 1
    this.localTableData[index].spIsPk = true
    this.pkData = []
    this.pkData = this.conversion.getPkMapping(this.localTableData)
    index = this.pkData.findIndex(
      (item) => item.srcId === toAddCol.srcId || item.spId == toAddCol.spId
    )
    this.pkArray.value.forEach((pk: IColumnTabData) => {
      if (pk.spIsPk) {
        newColumnOrder = newColumnOrder + 1
      }
      for (let i = 0; i < this.pkData.length; i++) {
        if (this.pkData[i].spId == pk.spId) {
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
      this.conv.SpSchema[this.currentObject!.id]?.PrimaryKeys.length == this.pkData.length
    ) {
      this.pkData.forEach((pk: IColumnTabData, i: number) => {
        if (
          this.pkData[i].spId === this.conv.SpSchema[this.currentObject!.id].PrimaryKeys[i].ColId
        ) {
          this.pkData[i].spOrder = this.conv.SpSchema[this.currentObject!.id].PrimaryKeys[i].Order
        } else {
          let index = this.conv.SpSchema[this.currentObject!.id].PrimaryKeys.map(
            (item) => item.ColId
          ).indexOf(pk.spId)
          pk.spOrder = this.conv.SpSchema[this.currentObject!.id].PrimaryKeys[index]?.Order
        }
      })
    } else {
      this.pkData.forEach((pk: IColumnTabData, i: number) => {
        let index = this.conv.SpSchema[this.currentObject!.id]?.PrimaryKeys.map(
          (item) => item.ColId
        ).indexOf(pk.spId)
        if (index !== -1) {
          pk.spOrder = this.conv.SpSchema[this.currentObject!.id]?.PrimaryKeys[index].Order
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
        if (typeof pk.spOrder === "number" && pk.spOrder < missingOrder) {
          pk.spOrder = Number(pk.spOrder) + 1
        }
      })
    }
  }

  getPkRequestObj() {
    let tableId: string = this.conv.SpSchema[this.currentObject!.id].Id
    let Columns: { ColId: string; Desc: boolean; Order: number }[] = []
    this.pkData.forEach((row: IColumnTabData) => {
      if (row.spIsPk)
        Columns.push({
          ColId: row.spId,
          Desc:
            typeof this.conv.SpSchema[this.currentObject!.id].PrimaryKeys.find(
              ({ ColId }) => ColId === row.spId
            ) !== 'undefined'
              ? this.conv.SpSchema[this.currentObject!.id].PrimaryKeys.find(
                ({ ColId }) => ColId === row.spId
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
      let interleaveTableId = this.tableInterleaveWith(this.currentObject?.id!)
      if (interleaveTableId != '' && this.isPKPrefixModified(this.currentObject?.id!, interleaveTableId)) {
        const dialogRef = this.dialog.open(InfodialogComponent, {
          data: {
            message:
              'Proceeding the update will remove interleaving between ' +
              this.currentObject?.name +
              ' and ' +
              this.conv.SpSchema[interleaveTableId].Name +
              ' tables.',
            title: 'Confirm Update',
            type: 'warning',
          },
          maxWidth: '500px',
        })
        dialogRef.afterClosed().subscribe((dialogResult) => {
          if (dialogResult) {
            let interleavedChildId: string =
              this.conv.SpSchema[this.currentObject!.id].ParentId != ''
                ? this.currentObject!.id
                : this.conv.SpSchema[interleaveTableId].Id
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
    let colId = this.localTableData[index].spId
    let synthColId = this.conv.SyntheticPKeys[this.currentObject!.id]
      ? this.conv.SyntheticPKeys[this.currentObject!.id].ColId
      : ''
    if (colId == synthColId) {
      const dialogRef = this.dialog.open(InfodialogComponent, {
        data: {
          message:
            'Removing this synthetic id column from primary key will drop the column from the table',
          title: 'Confirm removal of synthetic id',
          type: 'warning',
        },
        maxWidth: '500px',
      })
      dialogRef.afterClosed().subscribe((dialogResult) => {
        if (dialogResult) {
          this.dropPkHelper(index, element.value.spOrder)
        }
      })
    } else {
      this.dropPkHelper(index, element.value.spOrder)
    }
  }
  dropPkHelper(index: number, removedOrder: number) {
    this.localTableData[index].spIsPk = false
    this.pkData = []
    this.pkData = this.conversion.getPkMapping(this.localTableData)
    this.pkArray.value.forEach((pk: IColumnTabData) => {
      for (let i = 0; i < this.pkData.length; i++) {
        if (pk.spId == this.pkData[i].spId) {
          this.pkData[i].spOrder = pk.spOrder
          break
        }
      }
    })

    this.pkData.forEach((column: IColumnTabData, ind: number) => {
      if ( typeof column.spOrder === "number" && column.spOrder > removedOrder) {
        column.spOrder = Number(column.spOrder) - 1
      }
    })

    this.setAddPkColumnList()
    this.setPkRows()
  }

  setFkRows() {
    this.fkArray = this.fb.array([])
    var srcArr = new Array()
    var spArr = new Array()
    this.fkData.forEach((fk) => {
      srcArr.push({
        srcName: fk.srcName,
        srcColumns: fk.srcColumns,
        srcRefTable: fk.srcReferTable,
        srcRefColumns: fk.srcReferColumns,
        Id: fk.srcFkId,
      })
      if (fk.spName != '') {
        spArr.push({
          spName: fk.spName,
          spColumns: fk.spColumns,
          spRefTable: fk.spReferTable,
          spRefColumns: fk.spReferColumns,
          Id: fk.spFkId,
          spColIds: fk.spColIds,
          spReferColumnIds: fk.spReferColumnIds,
          spReferTableId: fk.spReferTableId,
        })
      }
    })
    for (let i = 0; i < Math.min(srcArr.length, spArr.length); i++) {
      this.fkArray.push(
        new FormGroup({
          srcFkId: new FormControl(srcArr[i].Id),
          spFkId: new FormControl(spArr[i].Id),
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
          spColIds: new FormControl(spArr[i].spColIds),
          spReferColumnIds: new FormControl(spArr[i].spReferColumnIds),
          spReferTableId: new FormControl(spArr[i].spReferTableId),
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
            spColIds: new FormControl([]),
            spReferColumnIds: new FormControl([]),
            spReferTableId: new FormControl(''),
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
        ColIds: fk.spColIds,
        ReferTableId: fk.spReferTableId,
        ReferColumnIds: fk.spReferColumnIds,
        Id: fk.spFkId,
      })
    })

    this.data.updateFkNames(this.currentObject!.id, spFkArr).subscribe({
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
        fk.spColIds = []
        fk.spReferColumnIds = []
        fk.spReferTableId = ''
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
    if (this.currentObject && !this.currentObject?.isDeleted && this.currentObject?.isSpannerNode) {
      this.data.getInterleaveConversionForATable(this.currentObject!.id)
    }
  }

  setInterleave() {
    this.data.setInterleave(this.currentObject!.id)
  }

  getInterleaveParentFromConv() {
    return this.currentObject?.type === ObjectExplorerNodeType.Table &&
      this.currentObject.isSpannerNode &&
      !this.currentObject.isDeleted &&
      this.conv.SpSchema[this.currentObject.id].ParentId != ''
      ? this.conv.SpSchema[this.conv.SpSchema[this.currentObject.id].ParentId]?.Name
      : null
  }

  setIndexRows() {
    this.spRowArray = this.fb.array([])
    const addedIndexColumns: string[] = this.localIndexData
      .map((data) => (data.spColName ? data.spColName : ''))
      .filter((name) => name != '')
    this.indexColumnNames = this.conv.SpSchema[this.currentObject!.parentId]?.ColIds?.filter(
      (colId) => {
        if (
          addedIndexColumns.includes(
            this.conv.SpSchema[this.currentObject!.parentId]?.ColDefs[colId]?.Name
          )
        ) {
          return false
        } else {
          return true
        }
      }
    ).map((colId: string) => this.conv.SpSchema[this.currentObject!.parentId]?.ColDefs[colId]?.Name)

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
    this.setIndexOrder()

    payload.push({
      Name: this.currentObject?.name || '',
      TableId: this.currentObject?.parentId || '',
      Unique: false,
      Keys: this.localIndexData
        .filter((idx: IIndexData) => {
          if (idx.spColId) return true
          return false
        })
        .map((col: any) => {
          return {
            ColId: col.spColId,
            Desc: col.spDesc,
            Order: col.spOrder,
          }
        }),
      Id: this.currentObject!.id,
    })

    if (payload[0].Keys.length == 0) {
      this.dropIndex()
    } else {
      this.data.updateIndex(this.currentObject?.parentId!, payload).subscribe({
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
          .dropIndex(this.currentObject!.parentId, this.currentObject!.id)
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
      this.localIndexData[index].spColId = ''
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
        spColName: this.addIndexKeyForm.value.columnName!,
        spDesc: this.addIndexKeyForm.value.ascOrDesc === 'desc',
        spOrder: spIndexCount + 1,
        srcColName: '',
        srcDesc: undefined,
        srcOrder: '',
        srcColId: undefined,
        spColId: this.currentObject
          ? this.conversion.getColIdFromSpannerColName(
            this.addIndexKeyForm.value.columnName!,
            this.currentObject.parentId,
            this.conv
          )
          : '',
      })
    this.setIndexRows()
  }

  restoreSpannerTable() {
    this.data
      .restoreTable(this.currentObject!.id)
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
          .dropTable(this.currentObject!.id)
          .pipe(take(1))
          .subscribe((res: string) => {
            if (res === '') {
              this.isObjectSelected = false
              this.data.getConversionRate()
              this.updateSidebar.emit(true)
            }
          })
        this.currentObject = null
      }
    })
  }

  tabChanged(tabChangeEvent: MatTabChangeEvent): void {
    this.currentTabIndex = tabChangeEvent.index
  }

  middleColumnToggle() {
    this.isMiddleColumnCollapse = !this.isMiddleColumnCollapse
    this.sidenav.setMiddleColComponent(this.isMiddleColumnCollapse)
  }

  tableInterleaveWith(table: string): string {
    if (this.conv.SpSchema[table].ParentId != '') {
      return this.conv.SpSchema[table].ParentId
    }
    let interleaveTable = ''
    Object.keys(this.conv.SpSchema).forEach((tableName: string) => {
      if (
        this.conv.SpSchema[tableName].ParentId != '' &&
        this.conv.SpSchema[tableName].ParentId == table
      ) {
        interleaveTable = tableName
      }
    })

    return interleaveTable
  }

  isPKPrefixModified(tableId: string, interleaveTableId: string): boolean {
    let parentPrimaryKey,childPrimaryKey: IIndexKey[]
    if (this.conv.SpSchema[tableId].ParentId != interleaveTableId) {
      parentPrimaryKey = this.pkObj.Columns
      childPrimaryKey = this.conv.SpSchema[interleaveTableId].PrimaryKeys
    } else {
      childPrimaryKey = this.pkObj.Columns
      parentPrimaryKey = this.conv.SpSchema[interleaveTableId].PrimaryKeys
    }

    for (let i = 0; i < parentPrimaryKey.length; i++) {
      for (let j = 0; j < childPrimaryKey.length; j++) {
        if (parentPrimaryKey[i].Order == childPrimaryKey[j].Order) {
          if (parentPrimaryKey[i].ColId != childPrimaryKey[j].ColId) {
            return true;
          }
        }
      }
    }
    return false
  }
}