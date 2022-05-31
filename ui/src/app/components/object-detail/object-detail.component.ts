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
import IConv, { IPrimaryKey } from 'src/app/model/conv'
import { DropIndexDialogComponent } from '../drop-index-dialog/drop-index-dialog.component'
import { ConversionService } from 'src/app/services/conversion/conversion.service'

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

  indexDisplayedColumns = ['srcIndexColName', 'srcIndexOrder', 'spIndexColName', 'spIndexOrder']
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
  addedColumnName: string = ''
  pkColumnNames: string[] = []
  indexColumnNames: string[] = []
  addPkColumnForm = new FormGroup({
    columnName: new FormControl('', [Validators.required]),
  })
  addIndexColumnForm = new FormGroup({
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
    this.rowArray = new FormArray([])
    this.pkData = this.conversion.getPkMapping(this.tableData)
    this.setPkOrder()
    this.pkColumnNames = []
    this.interleaveParentName = this.getParentFromDdl()

    if (this.currentObject?.type === ObjectExplorerNodeType.Table) {
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
      this.indexColumnNames = this.conv.SpSchema[this.currentObject?.parent].ColNames
      this.indexData.forEach((row: IIndexData) => {
        this.rowArray.push(
          new FormGroup({
            srcOrder: new FormControl(row.srcOrder),
            srcColName: new FormControl(row.srcColName),
            spOrder: new FormControl(row.spOrder),
            spColName: new FormControl(row.spColName),
          })
        )
      })
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
    this.pkData.forEach((row) => {
      if (row.srcIsPk && row.spIsPk) {
        this.pkArray.push(
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
      }
      if (!row.srcIsPk && row.spIsPk) {
        this.pkArray.push(
          new FormGroup({
            srcOrder: new FormControl(''),
            srcColName: new FormControl(''),
            srcDataType: new FormControl(''),
            srcIsPk: new FormControl(false),
            srcIsNotNull: new FormControl(false),
            spOrder: new FormControl(row.spOrder),
            spColName: new FormControl(row.spColName),
            spDataType: new FormControl(row.spDataType),
            spIsPk: new FormControl(row.spIsPk),
            spIsNotNull: new FormControl(row.spIsNotNull),
          })
        )
      }
      if (row.srcIsPk && !row.spIsPk) {
        this.pkArray.push(
          new FormGroup({
            srcOrder: new FormControl(row.srcOrder),
            srcColName: new FormControl(row.srcColName),
            srcDataType: new FormControl(row.srcDataType),
            srcIsPk: new FormControl(row.srcIsPk),
            srcIsNotNull: new FormControl(row.srcIsNotNull),
            spOrder: new FormControl(''),
            spColName: new FormControl(''),
            spDataType: new FormControl(''),
            spIsPk: new FormControl(false),
            spIsNotNull: new FormControl(false),
          })
        )
      }
    })
    this.pkDataSource = this.pkArray.controls
  }

  setPkColumn(columnName: string) {
    this.addedColumnName = columnName
  }

  addPkColumn() {
    let index = this.tableData.map((item) => item.spColName).indexOf(this.addedColumnName)
    this.tableData[index].spIsPk = true
    this.pkData = []
    this.pkData = this.conversion.getPkMapping(this.tableData)
    this.pkArray.value.forEach((pk: IColumnTabData, i: number) => {
      if (this.pkData[i].spOrder !== pk.spOrder && pk.spOrder) {
        this.pkData[i].spOrder = pk.spOrder
      }
    })
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
      this.conv.SpSchema[this.currentObject!.name].Pks.length == this.pkData.length
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
        let index = this.conv.SpSchema[this.currentObject!.name].Pks.map(
          (item) => item.Col
        ).indexOf(pk.spColName)
        pk.spOrder = this.conv.SpSchema[this.currentObject!.name].Pks[index].Order
      })
    }
  }

  getPkRequestObj() {
    let tableId: number = this.conv.SpSchema[this.currentObject!.name].Id
    let PrimaryKeyId: number = this.conv.SpSchema[this.currentObject!.name].PrimaryKeyId
    let Columns: { ColumnId: number; Desc: boolean; Order: number }[] = []
    this.pkArray.value.forEach((row: IColumnTabData) => {
      if (row.spIsPk)
        Columns.push({
          ColumnId: this.conv.SpSchema[this.currentObject!.name].ColDefs[row.spColName].Id,
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
    this.pkObj.tableId = tableId
    this.pkObj.Columns = Columns
    this.pkObj.PrimaryKeyId = PrimaryKeyId
  }

  togglePkEdit() {
    this.currentTabIndex = 1
    if (this.isPkEditMode) {
      this.getPkRequestObj()
      if (this.pkObj.Columns.length == 0) {
        this.dialog.open(InfodialogComponent, {
          data: { message: 'Add columns to the primary key for saving', type: 'error' },
          maxWidth: '500px',
        })
      }
      this.pkArray.value.forEach((pk: IColumnTabData, i: number) => {
        if (this.pkData[i].spOrder !== pk.spOrder) {
          this.pkData[i].spOrder = pk.spOrder
        }
      })
      this.data.updatePk(this.pkObj).subscribe({
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
      this.isPkEditMode = false
    } else {
      this.isPkEditMode = true
    }
  }

  dropPk(element: any) {
    let index = this.tableData.map((item) => item.spColName).indexOf(element.value.spColName)
    this.tableData[index].spIsPk = false
    this.pkData = []
    this.pkData = this.conversion.getPkMapping(this.tableData)
    this.pkArray.value.forEach((pk: IColumnTabData, i: number) => {
      if (typeof this.pkData[i] !== 'undefined' && this.pkData[i].spOrder !== pk.spOrder) {
        this.pkData[i].spOrder = pk.spOrder
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
    let ind: number = this.getRemovedFkIndex(element)
    this.data.dropFk(this.currentObject!.name, ind).subscribe({
      next: (res: string) => {
        if (res == '') {
          this.data.getDdl()
          console.log(element, 'element')

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
      this.ddlStmts[this.currentObject.name].includes(substr)
    ) {
      ddl = this.ddlStmts[this.currentObject.name].substring(
        this.ddlStmts[this.currentObject.name].indexOf(substr) + 20
      )
      return ddl.split(' ')[1]
    }
    return null
  }

  toggleIndexEdit() {
    if (this.isIndexEditMode) {
      this.isIndexEditMode = false
    } else {
      this.isIndexEditMode = true
    }
  }

  dropIndex() {
    let openDialog = this.dialog.open(DropIndexDialogComponent, {
      width: '35vw',
      minWidth: '450px',
      maxWidth: '600px',
      data: this.currentObject?.name,
    })
    openDialog.afterClosed().subscribe((res: string) => {
      if (res) {
        this.data
          .dropIndex(this.currentObject!.parent, this.currentObject!.pos)
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

  setIndexColumn(columnName: string) {}

  addIndexColumn() {
    alert('Add column implementation is in progress.')
  }

  tabChanged(tabChangeEvent: MatTabChangeEvent): void {
    this.currentTabIndex = tabChangeEvent.index
  }
}
