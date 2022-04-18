import { Component, OnInit } from '@angular/core'
import { FormArray, FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms'
import IConv, { ICreateIndex } from 'src/app/model/Conv'
import { DataService } from 'src/app/services/data/data.service'

@Component({
  selector: 'app-add-index-form',
  templateUrl: './add-index-form.component.html',
  styleUrls: ['./add-index-form.component.scss'],
})
export class AddIndexFormComponent implements OnInit {
  addIndexForm: FormGroup
  tableNames: string[] = []
  currentColumns: string[] = []
  conv: IConv = {} as IConv
  constructor(private fb: FormBuilder, private data: DataService) {
    this.addIndexForm = this.fb.group({
      tableName: ['', Validators.required],
      indexName: ['', Validators.required],
      ColsArray: this.fb.array([]),
    })
  }

  ngOnInit(): void {
    this.data.conv.subscribe({
      next: (res: IConv) => {
        this.conv = res
        this.tableNames = Object.keys(res.SpSchema)
      },
    })
  }

  get ColsArray() {
    return this.addIndexForm.controls['ColsArray'] as FormArray
  }

  selectedTableChage(tableName: string) {
    console.log(tableName)
    this.currentColumns = this.conv.SpSchema[tableName].ColNames
  }
  addNewColumnForm() {
    console.log(this.ColsArray)

    console.log('adding new to array')

    let newForm = this.fb.group({
      columnName: ['', Validators.required],
      sort: ['', Validators.required],
    })

    this.ColsArray.push(newForm)
  }

  AddIndex() {
    let idxData = this.addIndexForm.value
    let payload: ICreateIndex[] = []
    payload.push({
      Name: idxData.indexName,
      Table: idxData.tableName,
      Unique: false,
      Keys: idxData.ColsArray.map((col: any) => {
        return {
          Col: col.columnName,
          Desc: Boolean(col.sort),
        }
      }),
    })
    this.data.addIndex(idxData.tableName, payload)
  }
}
