import { Component, Input, OnInit } from '@angular/core'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'
import { FormControl, FormGroup, Validators } from '@angular/forms'
import { DataService } from 'src/app/services/data/data.service'
import IRule from 'src/app/model/rule'

@Component({
  selector: 'app-sidenav-rule',
  templateUrl: './sidenav-rule.component.html',
  styleUrls: ['./sidenav-rule.component.scss'],
})
export class SidenavRuleComponent implements OnInit {
  @Input() currentRules: any = []
  constructor(private sidenav: SidenavService, private data: DataService) { }

  ruleForm: FormGroup = new FormGroup({
    ruleName: new FormControl('', [Validators.required, Validators.pattern('^[a-zA-Z].{0,59}$')]),
    ruleType: new FormControl('', [Validators.required]),
  })

  rulename: string = ''
  ruletype: string = ''
  viewRuleData: any = []
  viewRuleFlag: boolean = false

  ngOnInit(): void {
    this.ruleForm.valueChanges.subscribe(() => {
      this.rulename = this.ruleForm.controls['ruleName']?.value
      this.ruletype = this.ruleForm.controls['ruleType']?.value
    })

    this.sidenav.displayRuleFlag.subscribe((flag: boolean) => {
      this.viewRuleFlag = flag
      if (this.viewRuleFlag) {
        this.sidenav.ruleData.subscribe((data: any) => {
          this.viewRuleData = data
          this.setViewRuleData(this.viewRuleData)
        })
      } else {
        this.ruleForm.enable()
        this.ruleForm.controls['ruleType'].setValue('')
        this.sidenav.sidenavRuleType.subscribe((res) => {
          if (res === 'addIndex') {
            this.ruleForm.controls['ruleType'].setValue('addIndex')
          }
        })
      }
    })
  }

  setViewRuleData(data: any) {
    this.ruleForm.disable()
    this.ruleForm.controls['ruleName'].setValue(data?.Name)
    this.ruleForm.controls['ruleType'].setValue(
      this.getViewRuleType(this.viewRuleData?.Type)
    )
  }

  closeSidenav(): void {
    this.sidenav.closeSidenav()
  }

  get ruleType() {
    return this.ruleForm.get('ruleType')?.value
  }
  resetRuleType() {
    this.ruleForm.controls['ruleType'].setValue('')
    this.ruleForm.controls['ruleName'].setValue('')
    this.ruleForm.markAsUntouched()
  }

  getViewRuleType(ruleType: any) {
    switch (ruleType) {
      case 'add_index': return 'addIndex'
      case 'global_datatype_change': return'globalDataType'
      case 'edit_column_max_length': return 'changeMaxLength'
      case 'apply_data_transformation': return 'applyDataTransformation'
    }
    return ''
  }
}
