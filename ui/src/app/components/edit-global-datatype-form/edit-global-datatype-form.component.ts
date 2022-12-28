import { Component, Input, OnInit, Output, EventEmitter } from '@angular/core'
import { FormBuilder, FormGroup, Validators } from '@angular/forms'
import IRule from 'src/app/model/rule'
import { DataService } from 'src/app/services/data/data.service'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'

interface IConvSourceType {
  T: string
  Brief: string
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
  ruleId: any = ''
  constructor(private fb: FormBuilder, private data: DataService, private sidenav: SidenavService) {
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
    this.addGlobalDataTypeForm.controls['destinationType'].setValue(Object.values(data?.Data)[0])
    this.addGlobalDataTypeForm.disable()
  }

  formSubmit(): void {
    const ruleValue = this.addGlobalDataTypeForm.value
    const source = ruleValue.sourceType
    const payload: Record<string, string> = {}
    payload[source] = ruleValue.destinationType
    this.applyRule(payload)
    this.resetRuleType.emit('')
    this.sidenav.closeSidenav()
  }

  // To dynamically change destination datatype.
  updateDestinationType(key: string): void {
    const desTypeDetail = this.conversionType[key]
    const desType: string[] = []
    desTypeDetail?.forEach((item: IConvSourceType) => {
      desType.push(item.T)
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
