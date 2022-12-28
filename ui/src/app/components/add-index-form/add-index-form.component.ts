import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core'
import { FormArray, FormBuilder, FormGroup, Validators } from '@angular/forms'
import IConv, { ICreateIndex } from 'src/app/model/conv'
import IRule from 'src/app/model/rule'
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
  @Input() ruleName: string = ''
  @Input() ruleType: string = ''
  @Output() resetRuleType: EventEmitter<any> = new EventEmitter<any>()
  addIndexForm: FormGroup
  tableNames: string[] = []
  totalColumns: string[] = []
  addColumnsList: string[][] = []
  commonColumns: string[] = []
  viewRuleData: IRule = {}
  viewRuleFlag: boolean = false
  conv: IConv = {} as IConv
  ruleId: any = ''
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

    this.sidenav.displayRuleFlag.subscribe((flag: boolean) => {
      this.viewRuleFlag = flag
      if (this.viewRuleFlag) {
        this.sidenav.ruleData.subscribe((data: IRule) => {
          this.viewRuleData = data
          if (this.viewRuleData && this.viewRuleFlag) {
            this.getRuleData(this.viewRuleData)
          }
        })
      }
    })
  }

  getRuleData(data: IRule) {
    this.ruleId = data?.Id
    let tableName: string = this.conv.SpSchema[data?.Data?.TableId]?.Name
    this.addIndexForm.controls['tableName'].setValue(tableName)
    this.addIndexForm.controls['indexName'].setValue(data?.Data?.Name)
    this.selectedTableChange(tableName)
    this.setColArraysForViewRules(data?.Data?.TableId, data?.Data?.Keys)
    this.addIndexForm.disable()
  }

  setColArraysForViewRules(tableId: string, data: any) {
    this.ColsArray.clear()
    if (!data) {
      return
    }
    for (let i = 0; i < data?.length; i++) {
      this.updateCommonColumns()
      this.addColumnsList.push([...this.commonColumns])

      let columnName: string = this.conv.SpSchema[tableId]?.ColDefs[data[i].ColId].Name

      let newForm = this.fb.group({
        columnName: [columnName, Validators.required],
        sort: [data[i].Desc.toString(), Validators.required],
      })

      this.ColsArray.push(newForm)
    }
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

    this.applyRule(payload[0])
    this.resetRuleType.emit('')
    this.sidenav.setSidenavAddIndexTable('')
    this.sidenav.closeSidenav()
  }

  applyRule(data: ICreateIndex) {
    let idxData = this.addIndexForm.value
    let tableId: string = this.conversion.getTableIdFromSpName(idxData.tableName, this.conv)
    let payload: IRule = {
      Name: this.ruleName,
      Type: 'add_index',
      ObjectType: 'Table',
      AssociatedObjects: tableId,
      Enabled: true,
      Data: data,
      Id: '',
    }
    this.data.applyRule(payload)
  }

  deleteRule() {
    this.data.dropRule(this.ruleId)
    this.resetRuleType.emit('')
    this.sidenav.setSidenavAddIndexTable('')
    this.sidenav.closeSidenav()
  }
}
