import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core'
import { FormArray, FormBuilder, FormGroup, Validators } from '@angular/forms'
import IConv, { ICreateIndex } from 'src/app/model/conv'
import { DataService } from 'src/app/services/data/data.service'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'

@Component({
  selector: 'app-add-index-form',
  templateUrl: './add-index-form.component.html',
  styleUrls: ['./add-index-form.component.scss'],
})
export class AddIndexFormComponent implements OnInit {
  @Input() ruleNameValid: boolean = false
  @Output() resetRuleType: EventEmitter<any> = new EventEmitter<any>()
  addIndexForm: FormGroup
  tableNames: string[] = []
  currentColumns: string[] = []
  conv: IConv = {} as IConv
  constructor(private fb: FormBuilder, private data: DataService, private sidenav: SidenavService) {
    this.addIndexForm = this.fb.group({
      tableName: ['', Validators.required],
      indexName: ['', [Validators.required, Validators.pattern('^[a-zA-Z].{0,59}$')]],
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
    this.sidenav.sidenavAddIndexTable.subscribe({
      next: (res: string) => {
        this.addIndexForm.controls['tableName'].setValue(res)
        if (res !== '') this.selectedTableChange(res)
      },
    })
  }

  get ColsArray() {
    return this.addIndexForm.controls['ColsArray'] as FormArray
  }

  selectedTableChange(tableName: string) {
    this.currentColumns = this.conv.SpSchema[tableName].ColNames
  }
  addNewColumnForm() {
    let newForm = this.fb.group({
      columnName: ['', Validators.required],
      sort: ['', Validators.required],
    })

    this.ColsArray.push(newForm)
  }

  addIndex() {
    let idxData = this.addIndexForm.value
    let payload: ICreateIndex[] = []
    payload.push({
      Name: idxData.indexName,
      Table: idxData.tableName,
      Unique: false,
      Keys: idxData.ColsArray.map((col: any) => {
        return {
          Col: col.columnName,
          Desc: col.sort === 'true',
        }
      }),
    })
    this.data.addIndex(idxData.tableName, payload)
    this.resetRuleType.emit('')
    this.sidenav.setSidenavAddIndexTable('')
    this.sidenav.closeSidenav()
  }
}
