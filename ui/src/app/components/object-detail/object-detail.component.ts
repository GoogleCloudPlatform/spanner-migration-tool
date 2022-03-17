import { Component, Input, OnInit, SimpleChanges } from '@angular/core'
import { FormArray, FormControl, FormGroup } from '@angular/forms'
import IUpdateTable from './../../model/updateTable'
import { DataService } from 'src/app/services/data/data.service'
import { MatDialog } from '@angular/material/dialog'
import { InfodialogComponent } from '../infodialog/infodialog.component'
import { LoaderService } from '../../services/loader/loader.service'
interface IColMap {
  srcColName: string
  srcDataType: string
  spColName: string
  spDataType: string
}
@Component({
  selector: 'app-object-detail',
  templateUrl: './object-detail.component.html',
  styleUrls: ['./object-detail.component.scss'],
})
export class ObjectDetailComponent implements OnInit {
  constructor(
    private data: DataService,
    private dialog: MatDialog,
    private loader: LoaderService
  ) {}

  ngOnInit(): void {}

  @Input() tableName: string = ''
  @Input() typeMap: any = {}
  @Input() ddlStmts: any = {}
  @Input() rowData: IColMap[] = []
  displayedColumns = ['srcColName', 'srcDataType', 'spColName', 'spDataType']
  dataSource: any = []
  isEditMode: boolean = false
  rowArray: FormArray = new FormArray([])

  ngOnChanges(changes: SimpleChanges): void {
    this.tableName = changes['tableName']?.currentValue || this.tableName
    this.rowData = changes['rowData']?.currentValue || this.rowData
    this.isEditMode = false
    this.rowArray = new FormArray([])
    this.rowData.forEach((row) => {
      this.rowArray.push(
        new FormGroup({
          srcColName: new FormControl(row.srcColName),
          srcDataType: new FormControl(row.srcDataType),
          spColName: new FormControl(row.spColName),
          spDataType: new FormControl(row.spDataType),
        })
      )
    })
    this.dataSource = this.rowArray.controls
  }

  toggleEdit() {
    if (this.isEditMode) {
      let updateData: IUpdateTable = { UpdateCols: {} }
      this.rowArray.value.forEach((col: IColMap, i: number) => {
        let oldRow = this.rowData[i]
        updateData.UpdateCols[this.rowData[i].spColName] = {
          Rename: oldRow.spColName !== col.spColName ? col.spColName : '',
          NotNull: true ? 'ADDED' : 'REMOVED',
          PK: '',
          Removed: false,
          ToType: oldRow.spDataType !== col.spDataType ? col.spDataType : '',
        }
      })
      this.data.updateTable(this.tableName, updateData).subscribe({
        next: (res: string) => {
          console.log(res)
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
}
