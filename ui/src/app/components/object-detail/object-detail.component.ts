import { Component, Input, OnInit, SimpleChanges } from '@angular/core'
import { FormArray, FormControl, FormGroup } from '@angular/forms'

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
  constructor() {}

  ngOnInit(): void {}

  @Input() tableName: string = ''
  @Input() typeMap: any = {}
  @Input() rowData: IColMap[] = []
  displayedColumns = ['srcColName', 'srcDataType', 'spColName', 'spDataType']
  dataSource: any = []
  isEditMode: boolean = false
  rowArray: FormArray = new FormArray([])

  ngOnChanges(changes: SimpleChanges): void {
    this.tableName = changes['tableName']?.currentValue || this.tableName
    this.rowData = changes['rowData']?.currentValue || this.rowData

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
      this.isEditMode = false
    } else {
      this.isEditMode = true
    }
  }
}
