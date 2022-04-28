import { Component, EventEmitter, Input, OnInit, Output, SimpleChanges } from '@angular/core'
import { FormArray, FormControl, FormGroup } from '@angular/forms'
import IUpdateTable from './../../model/updateTable'
import { DataService } from 'src/app/services/data/data.service'
import { MatDialog } from '@angular/material/dialog'
import { InfodialogComponent } from '../infodialog/infodialog.component'
import IColumnTabData, { IIndexData } from '../../model/EditTable'
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service'
import IFkTabData from 'src/app/model/FkTabData'
import { ObjectExplorerNodeType, StorageKeys } from 'src/app/app.constants'
import FlatNode from 'src/app/model/SchemaObjectNode'
import { take } from 'rxjs'
import { MatTabChangeEvent } from '@angular/material/tabs/tab-group'

@Component({
  selector: 'app-object-detail',
  templateUrl: './object-detail.component.html',
  styleUrls: ['./object-detail.component.scss'],
})
export class ObjectDetailComponent implements OnInit {
  constructor(
    private data: DataService,
    private dialog: MatDialog,
    private snackbar: SnackbarService
  ) {}

  ObjectExplorerNodeType = ObjectExplorerNodeType

  ngOnInit(): void {}

  @Input() currentObject: FlatNode | null = null
  @Input() typeMap: any = {}
  @Input() ddlStmts: any = {}
  @Input() fkData: IFkTabData[] = []
  @Input() tableData: IColumnTabData[] = []
  @Input() indexData: IIndexData[] = []
  @Output() updateSidebar = new EventEmitter<boolean>()

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
  ]

  indexDisplayedColumns = ['srcIndexColName', 'srcIndexOrder', 'spIndexColName', 'spIndexOrder']
  dataSource: any = []
  fkDataSource: any = []
  isEditMode: boolean = false
  isFkEditMode: boolean = false
  isIndexEditMode: boolean = false
  isObjectSelected: boolean = false
  rowArray: FormArray = new FormArray([])
  fkArray: FormArray = new FormArray([])
  srcDbName: string = localStorage.getItem(StorageKeys.SourceDbName) as string
  isSpTableSuggesstionDisplay: boolean[] = []
  spTableSuggestion: string[] = []
  currentTabIndex: number = 0

  ngOnChanges(changes: SimpleChanges): void {
    this.fkData = changes['fkData']?.currentValue || this.fkData
    this.currentObject = changes['currentObject']?.currentValue || this.currentObject
    this.tableData = changes['tableData']?.currentValue || this.tableData
    this.indexData = changes['indexData']?.currentValue || this.indexData

    this.isObjectSelected = this.currentObject ? true : false
    this.isEditMode = false
    this.isFkEditMode = false
    this.rowArray = new FormArray([])
    if (this.currentObject?.type === ObjectExplorerNodeType.Table) {
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
    } else {
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
          this.snackbar.openSnackBar(`${colName} column dropped successfully`, 'close', 4000)
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

  toggleFkEdit() {
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
      this.isFkEditMode = true
    }
  }

  dropFk(element: any) {
    console.log(element)
    console.log(element.get('spName').value)
    let ind: number = this.getRemovedFkIndex(element)
    this.data.dropFk(this.currentObject!.name, ind).subscribe({
      next: (res: string) => {
        if (res == '') {
          this.data.getDdl()
          this.snackbar.openSnackBar(
            `${element.get('spName')} Foreign key dropped successfully`,
            'close',
            4000
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

  toggleIndexEdit() {
    if (this.isIndexEditMode) {
      let updatedIndexNames: Record<string, string> = {}

      this.rowArray.value.forEach((Index: IIndexData, i: number) => {
        let oldIndex = this.indexData[i]
        if (oldIndex.spColName !== Index.spColName) {
          updatedIndexNames[oldIndex.spColName] = Index.spColName
        }
      })

      this.data.updateIndexNames(this.currentObject!.name, updatedIndexNames).subscribe({
        next: (res: string) => {
          if (res == '') {
            this.isIndexEditMode = false
          } else {
            this.dialog.open(InfodialogComponent, {
              data: { message: res, type: 'error' },
              maxWidth: '500px',
            })
          }
        },
      })
    } else {
      this.isIndexEditMode = true
    }
  }

  dropIndex() {
    this.data
      .dropIndex(this.currentObject!.parent, this.currentObject!.pos)
      .pipe(take(1))
      .subscribe((res: string) => {
        console.log(res)
        if (res === '') {
          this.isObjectSelected = false
          this.updateSidebar.emit(true)
        }
      })
  }

  tabChanged(tabChangeEvent: MatTabChangeEvent): void {
    this.currentTabIndex = tabChangeEvent.index
    // console.log('tabChangeEvent => ', tabChangeEvent)
    // console.log('index => ', tabChangeEvent.index)
  }
}
