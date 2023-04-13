import { Component, Input, OnInit, Output, EventEmitter } from '@angular/core'
import { FormBuilder, FormGroup, Validators } from '@angular/forms'
import { Dialect } from 'src/app/app.constants'
import IConv from 'src/app/model/conv'
import IRule from 'src/app/model/rule'
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
  ruleId: any
  pgSQLToStandardTypeTypemap: Map<String, String> = new Map()
  standardTypeToPGSQLTypemap: Map<String, String> = new Map()
  conv: IConv = {} as IConv
  isPostgreSQLDialect: boolean = false
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

    this.data.conv.subscribe({
      next: (res: IConv) => {
        this.conv = res
        this.isPostgreSQLDialect = this.conv.SpDialect === Dialect.PostgreSQLDialect
      },
    })

    this.conversion.pgSQLToStandardTypeTypeMap.subscribe((typemap) => {
      this.pgSQLToStandardTypeTypemap = typemap
    })
    this.conversion.standardTypeToPGSQLTypeMap.subscribe((typemap) => {
      this.standardTypeToPGSQLTypemap = typemap
    })

    this.sidenav.displayRuleFlag.subscribe((flag: boolean) => {
      this.viewRuleFlag = flag
      if (this.viewRuleFlag) {
        this.sidenav.ruleData.subscribe((data: IRule) => {
          this.viewRuleData = data
          if (this.viewRuleData) {
            this.setViewRuleData(this.viewRuleData)
          }
        })
      }
    })
  }

  setViewRuleData(data: IRule) {
    this.ruleId = data?.Id
    this.addGlobalDataTypeForm.controls['sourceType'].setValue(Object.keys(data?.Data)[0])
    this.updateDestinationType(Object.keys(data?.Data)[0])
    if (this.isPostgreSQLDialect) {
      let pgSQLType = this.standardTypeToPGSQLTypemap.get(Object.values(this.viewRuleData?.Data)[0] as string)
      this.addGlobalDataTypeForm.controls['destinationType'].setValue(
        pgSQLType === undefined ? Object.values(this.viewRuleData?.Data)[0] : pgSQLType
      )
    } else {
      this.addGlobalDataTypeForm.controls['destinationType'].setValue(Object.values(this.viewRuleData?.Data)[0])
    }
    this.addGlobalDataTypeForm.disable()
  }

  formSubmit(): void {
    const ruleValue = this.addGlobalDataTypeForm.value
    const source = ruleValue.sourceType
    const payload: Record<string, string> = {}

    if (this.isPostgreSQLDialect) {
      let destinationType = this.pgSQLToStandardTypeTypemap.get(ruleValue.destinationType)
      payload[source] = destinationType === undefined ? ruleValue.destinationType : destinationType
    } else {
      payload[source] = ruleValue.destinationType
    }

    this.applyRule(payload)
    this.resetRuleType.emit('')
    this.sidenav.closeSidenav()
  }

  // To dynamically change destination datatype.
  updateDestinationType(key: string): void {
    const desTypeDetail = this.conversionType[key]
    const desType: string[] = []
    desTypeDetail?.forEach((item: IConvSourceType) => {
      desType.push(item.DisplayT)
    })
    this.destinationType = desType
  }

  applyRule(data: Record<string, string>) {
    let payload: IRule = {
      Name: this.ruleName,
      Type: 'global_datatype_change',
      ObjectType: 'Column',
      AssociatedObjects: 'All Columns',
      Enabled: true,
      Data: data,
      Id: '',
    }

    this.data.applyRule(payload)
  }

  deleteRule() {
    this.data.dropRule(this.ruleId)
    this.resetRuleType.emit('')
    this.sidenav.closeSidenav()
  }
}
