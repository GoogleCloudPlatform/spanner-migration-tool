import { Component, OnInit } from '@angular/core'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'
import { FormControl, FormControlName, FormGroup, Validators } from '@angular/forms'
import { DataService } from 'src/app/services/data/data.service'
import IRuleContent from 'src/app/model/Rule'

@Component({
  selector: 'app-sidenav-rule',
  templateUrl: './sidenav-rule.component.html',
  styleUrls: ['./sidenav-rule.component.scss'],
})
export class SidenavRuleComponent implements OnInit {
  constructor(private sidenavService: SidenavService, private data: DataService) {}

  ruleForm: FormGroup = new FormGroup({
    ruleName: new FormControl('', [Validators.required]),
    ruleType: new FormControl('AddIndex', [Validators.required]),
  })

  ngOnInit(): void {}
  closeSidenav(): void {
    this.sidenavService.closeSidenav()
  }
  get ruleType() {
    return this.ruleForm.get('ruleType')?.value
  }
}
