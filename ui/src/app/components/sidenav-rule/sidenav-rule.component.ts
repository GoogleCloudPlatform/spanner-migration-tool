import { Component, OnInit } from '@angular/core'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'
import { FormArray, FormControl, FormControlName, FormGroup, Validators } from '@angular/forms'
import { DataService } from 'src/app/services/data/data.service'
import IRuleContent from 'src/app/model/Rule'

interface IConvSourceType {
  T: string
  Brief: string
}

@Component({
  selector: 'app-sidenav-rule',
  templateUrl: './sidenav-rule.component.html',
  styleUrls: ['./sidenav-rule.component.scss'],
})
export class SidenavRuleComponent implements OnInit {
  constructor(private sidenavService: SidenavService, private data: DataService) {}
  conversionType: Record<string, IConvSourceType[]> = {}
  sourceType: string[] = []
  destinationType: string[] = []

  ruleForm: FormGroup = new FormGroup({
    ruleName: new FormControl('', [Validators.required]),
    ruleType: new FormControl([Validators.required]),
    ruleContent: new FormGroup({
      objectType: new FormControl('column', [Validators.required]),
      table: new FormControl('allTable', [Validators.required]),
      column: new FormControl('allColumn', [Validators.required]),
      sourceType: new FormControl([Validators.required]),
      destinationType: new FormControl([Validators.required]),
    }),
  })

  ngOnInit(): void {
    this.data.typeMap.subscribe({
      next: (res) => {
        this.conversionType = res
        this.sourceType = Object.keys(this.conversionType)
      },
    })
  }
  closeSidenav(): void {
    this.sidenavService.closeSidenav()
  }
  formSubmit(): void {
    const ruleValue = this.ruleForm.value
    const ruleContentValue = ruleValue.ruleContent

    const source = ruleContentValue.sourceType
    const payload: Record<string, string> = {}
    payload[source] = ruleContentValue.destinationType

    const nextData: IRuleContent = {
      name: ruleValue.ruleName,
      type: ruleValue.ruleType,
      objectType: ruleContentValue.objectType,
      associatedObject: 'All tables',
      enabled: true,
    }
    this.data.updateGlobalType(payload)
    this.data.addRule(nextData)
    this.closeSidenav()
  }
  //To dynamically change destination select option
  updateDestinationType(key: string): void {
    const desTypeDetial = this.conversionType[key]
    const desType: string[] = []
    desTypeDetial.forEach((item: IConvSourceType) => {
      desType.push(item.T)
    })
    this.destinationType = desType
  }
}
