import { Component, Input, OnInit } from '@angular/core'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'
import { FormControl, FormGroup, Validators } from '@angular/forms'
import { DataService } from 'src/app/services/data/data.service'

@Component({
  selector: 'app-sidenav-rule',
  templateUrl: './sidenav-rule.component.html',
  styleUrls: ['./sidenav-rule.component.scss'],
})
export class SidenavRuleComponent implements OnInit {
  @Input() currentRules: any = []
  constructor(private sidenav: SidenavService, private data: DataService) {}

  ruleForm: FormGroup = new FormGroup({
    ruleName: new FormControl('', [Validators.required, Validators.pattern('^[a-zA-Z].{0,59}$')]),
    ruleType: new FormControl('', [Validators.required]),
  })
  rulename: string = ''
  ruletype: string = ''

  ngOnInit(): void {
    this.sidenav.sidenavRuleType.subscribe((res) => {
      if (res === 'addIndex') {
        this.ruleForm.controls['ruleType'].setValue('addIndex')
      }
    })

    this.ruleForm.valueChanges.subscribe((val) => {
      this.rulename = this.ruleForm.controls['ruleName']?.value
      this.ruletype = this.ruleForm.controls['ruleType']?.value
    })
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
}
