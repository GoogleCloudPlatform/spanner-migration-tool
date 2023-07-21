import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';
import { FormGroup, FormBuilder, Validators } from '@angular/forms';
import { IShardIdPrimaryKey } from 'src/app/model/conv';
import IRule from 'src/app/model/rule';
import { DataService } from 'src/app/services/data/data.service';
import { SidenavService } from 'src/app/services/sidenav/sidenav.service';

@Component({
  selector: 'app-add-shard-id-primary-key',
  templateUrl: './add-shard-id-primary-key.component.html',
  styleUrls: ['./add-shard-id-primary-key.component.scss']
})
export class AddShardIdPrimaryKeyComponent implements OnInit {
  @Input() ruleNameValid: boolean = false
  @Input() ruleName: string = ''
  @Input() ruleType: string = ''
  @Output() resetRuleType: EventEmitter<any> = new EventEmitter<any>()
  addShardIdPrimaryKeyForm: FormGroup
  ruleId: any
  viewRuleFlag: boolean = false
  viewRuleData: any = {}

  primaryKeyOrder = [
    { value: true, display: 'At the beginning'},
    { value: false, display: 'At the end'},
  ]

  constructor(private fb: FormBuilder, private data: DataService, private sidenav: SidenavService) {
    this.addShardIdPrimaryKeyForm = this.fb.group({
      table: ['allTable', Validators.required],
      primaryKeyOrder: ['', Validators.required],
    })
   }

  ngOnInit(): void {
    this.sidenav.displayRuleFlag.subscribe((flag: boolean) => {
      this.viewRuleFlag = flag
      if (this.viewRuleFlag) {
        this.sidenav.ruleData.subscribe((data: IRule) => {
          this.viewRuleData = data
          if (this.viewRuleData) {
            this.setViewRuleData(this.viewRuleData)
          }
        })
        this.addShardIdPrimaryKeyForm.disable()
      }
    })
  }


  formSubmit(): void {
    const ruleValue = this.addShardIdPrimaryKeyForm.value
    let data:IShardIdPrimaryKey = {
      AddedAtTheStart: ruleValue.primaryKeyOrder
    }
    let payload: IRule = {
      Name: this.ruleName,
      Type: 'add_shard_id_primary_key',
      AssociatedObjects: 'All Tables',
      Enabled: true,
      Data: data,
      Id: '',
    }

    this.data.applyRule(payload)
    this.resetRuleType.emit('')
    this.sidenav.closeSidenav()
  }

  setViewRuleData(data: IRule) {
    this.ruleId = data?.Id
    this.addShardIdPrimaryKeyForm.controls['primaryKeyOrder'].setValue(data?.Data?.AddedAtTheStart)
  }

  deleteRule() {
    this.data.dropRule(this.ruleId)
    this.resetRuleType.emit('')
    this.sidenav.closeSidenav()
  }

}
