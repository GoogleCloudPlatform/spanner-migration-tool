import { Component, Input, OnInit, Output, EventEmitter } from '@angular/core'
import { FormBuilder, FormGroup, Validators } from '@angular/forms'
import { IRule } from 'src/app/model/rule'
import { ConversionService } from 'src/app/services/conversion/conversion.service'
import { DataService } from 'src/app/services/data/data.service'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'

interface IConvSourceType {
  T: string
  Brief: string
  DisplayT: string
}

@Component({
  selector: 'app-edit-global-datatype-form',
  templateUrl: './edit-global-datatype-form.component.html',
  styleUrls: ['./edit-global-datatype-form.component.scss'],
})
export class EditGlobalDatatypeFormComponent implements OnInit {
  @Input() ruleNameValid: boolean = false
  @Input() ruleType: string = ''
  @Input() ruleName: string = ''
  @Output() resetRuleType: EventEmitter<any> = new EventEmitter<any>()
  addGlobalDataTypeForm: FormGroup
  conversionType: Record<string, IConvSourceType[]> = {}
  sourceType: string[] = []
  destinationType: string[] = []
  viewRuleData: any = []
  viewRuleFlag: boolean = false
  ruleId: string = ''
  pgSQLToStandardTypeTypemap: Map<String, String> = new Map()
  standardTypeToPGSQLTypemap: Map<String, String> = new Map()
  constructor(private fb: FormBuilder, private data: DataService, private sidenav: SidenavService, private conversion: ConversionService, private fetch: FetchService) {
    this.addGlobalDataTypeForm = this.fb.group({
      objectType: ['column', Validators.required],
      table: ['allTable', Validators.required],
      column: ['allColumn', Validators.required],
      sourceType: ['', Validators.required],
      destinationType: ['', Validators.required],
    })
  }

  ngOnInit(): void {
    this.data.typeMap.subscribe({
      next: (res) => {
        this.conversionType = res
        this.sourceType = Object.keys(this.conversionType)
      },
    })

    this.conversion.pgSQLToStandardTypeTypeMap.subscribe((typemap) => {
      this.pgSQLToStandardTypeTypemap = typemap
    })
    this.conversion.standardTypeToPGSQLTypeMap.subscribe((typemap) => {
      this.standardTypeToPGSQLTypemap = typemap
    })
    this.sidenav.passRules.subscribe(([data, flag]: any) => {
      this.viewRuleData = data
      this.viewRuleFlag = flag

      if (this.viewRuleFlag) {
        this.ruleId = this.viewRuleData?.Id
        this.addGlobalDataTypeForm.controls['sourceType'].setValue(
          Object.keys(this.viewRuleData?.Data)[0]
        )
        this.updateDestinationType(Object.keys(this.viewRuleData?.Data)[0])
        let pgSQLType = this.standardTypeToPGSQLTypemap.get(Object.values(this.viewRuleData?.Data)[0] as string)
        
        this.addGlobalDataTypeForm.controls['destinationType'].setValue(
          pgSQLType === undefined ? Object.values(this.viewRuleData?.Data)[0] : pgSQLType
        )
        this.addGlobalDataTypeForm.disable()
      }
    })
  }

  formSubmit(): void {
    const ruleValue = this.addGlobalDataTypeForm.value
    const source = ruleValue.sourceType
    const payload: Record<string, string> = {}
    
    let destinationType = this.pgSQLToStandardTypeTypemap.get(ruleValue.destinationType)
    payload[source] = destinationType === undefined ? ruleValue.destinationType : destinationType
    this.applyRule(payload)
    this.resetRuleType.emit('')
    this.sidenav.closeSidenav()
  }

  // To dynamically change destination datatype.
  updateDestinationType(key: string): void {
    const desTypeDetail = this.conversionType[key]
    const desType: string[] = []
    desTypeDetail.forEach((item: IConvSourceType) => {
      desType.push(item.DisplayT)
    })
    this.destinationType = desType
  }

  applyRule(data: Record<string, string>) {
    let payload: IRule = {
      name: this.ruleName,
      type: 'global_datatype_change',
      objectType: 'Column',
      associatedObjects: 'All Columns',
      enabled: true,
      data: data,
    }

    this.data.applyRule(payload)
  }

  deleteRule() {
    this.data.dropRule(this.ruleId)
    this.resetRuleType.emit('')
    this.sidenav.closeSidenav()
  }
}
