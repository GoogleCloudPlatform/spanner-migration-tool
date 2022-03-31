import { Component, Input, OnInit, SimpleChanges } from '@angular/core'
import { FormArray, FormControl, FormGroup } from '@angular/forms'
import IUpdateTable from './../../model/updateTable'
import { DataService } from 'src/app/services/data/data.service'
import { MatDialog } from '@angular/material/dialog'
import { InfodialogComponent } from '../infodialog/infodialog.component'
import { LoaderService } from '../../services/loader/loader.service'
import IColumnTabData from '../../model/ColumnTabData'
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service'
import { StorageKeys } from 'src/app/app.constants'

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

  ngOnInit(): void {}

  @Input() tableName: string = ''
  @Input() typeMap: any = {}
  @Input() ddlStmts: any = {}
  @Input() rowData: IColumnTabData[] = []

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
  dataSource: any = []
  isEditMode: boolean = false
  isTableSelected: boolean = false
  rowArray: FormArray = new FormArray([])
  srcDbName: string = localStorage.getItem(StorageKeys.SourceDbName) as string

  ngOnChanges(changes: SimpleChanges): void {
    this.tableName = changes['tableName']?.currentValue || this.tableName
    this.rowData = changes['rowData']?.currentValue || this.rowData

    this.isTableSelected = this.tableName === '' ? false : true
    this.isEditMode = false
    this.rowArray = new FormArray([])
    this.rowData.forEach((row) => {
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
    this.dataSource = this.rowArray.controls
  }

  toggleEdit() {
    if (this.isEditMode) {
      let updateData: IUpdateTable = { UpdateCols: {} }
      this.rowArray.value.forEach((col: IColumnTabData, i: number) => {
        let oldRow = this.rowData[i]
        updateData.UpdateCols[this.rowData[i].spColName] = {
          Rename: oldRow.spColName !== col.spColName ? col.spColName : '',
          NotNull: col.spIsNotNull ? 'ADDED' : 'REMOVED',
          PK: '',
          Removed: false,
          ToType: oldRow.spDataType !== col.spDataType ? col.spDataType : '',
        }
      })
      this.data.updateTable(this.tableName, updateData).subscribe({
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
      updateData.UpdateCols[this.rowData[i].spColName] = {
        Rename: '',
        NotNull: '',
        PK: '',
        Removed: col.spColName === colName ? true : false,
        ToType: '',
      }
    })
    this.data.updateTable(this.tableName, updateData).subscribe({
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
}
