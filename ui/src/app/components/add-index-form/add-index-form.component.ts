import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core'
import { FormArray, FormBuilder, FormGroup, Validators } from '@angular/forms'
import IConv, { ICreateIndex } from 'src/app/model/conv'
import { DataService } from 'src/app/services/data/data.service'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'
import { ConversionService } from 'src/app/services/conversion/conversion.service'

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
  totalColumns: string[] = []
  addColumnsList: string[][] = []
  commonColumns: string[] = []
  conv: IConv = {} as IConv
  constructor(
    private fb: FormBuilder,
    private data: DataService,
    private sidenav: SidenavService,
    private conversion: ConversionService
  ) {
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
        this.tableNames = Object.keys(res.SpSchema).map(
          (talbeId: string) => res.SpSchema[talbeId].Name
        )
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
    let tableId = this.conversion.getTableIdFromSpName(tableName, this.conv)
    if (tableId) {
      let spTableData = this.conv.SpSchema[tableId]
      this.totalColumns = this.conv.SpSchema[tableId].ColIds.map(
        (colId: string) => spTableData.ColDefs[colId].Name
      )
    }
    this.ColsArray.clear()
    this.commonColumns = []
    this.addColumnsList = []
    this.updateCommonColumns()
  }
  addNewColumnForm() {
    let newForm = this.fb.group({
      columnName: ['', Validators.required],
      sort: ['', Validators.required],
    })
    this.ColsArray.push(newForm)
    this.updateCommonColumns()
    this.addColumnsList.push([...this.commonColumns])
  }

  selectedColumnChange() {
    this.updateCommonColumns()
    this.addColumnsList = this.addColumnsList.map((_, i) => {
      const columns: string[] = [...this.commonColumns]
      if (this.ColsArray.value[i].columnName !== '')
        columns.push(this.ColsArray.value[i].columnName)
      return columns
    })
  }

  updateCommonColumns() {
    this.commonColumns = this.totalColumns.filter((columnName) => {
      let flag = true
      this.ColsArray.value.forEach((col: any) => {
        if (col.columnName === columnName) flag = false
      })
      return flag
    })
  }

  removeColumnForm(index: number) {
    this.ColsArray.removeAt(index)
    this.addColumnsList = this.addColumnsList.filter((_, i) => {
      if (i === index) return false
      else return true
    })
    this.selectedColumnChange()
  }

  addIndex() {
    let idxData = this.addIndexForm.value
    let payload: ICreateIndex[] = []
    let tableId = this.conversion.getTableIdFromSpName(idxData.tableName, this.conv)
    payload.push({
      Name: idxData.indexName,
      TableId: tableId,
      Unique: false,
      Keys: idxData.ColsArray.map((col: any, i: number) => {
        let colId: string = this.conversion.getColIdFromSpannerColName(
          col.columnName,
          tableId,
          this.conv
        )
        return {
          ColId: colId,
          Desc: col.sort === 'true',
          Order: i + 1,
        }
      }),
      Id: '',
    })
    this.data.addIndex(tableId, payload)
    this.resetRuleType.emit('')
    this.sidenav.setSidenavAddIndexTable('')
    this.sidenav.closeSidenav()
  }
}
