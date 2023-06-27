import { Component, EventEmitter, Input, OnInit, Output } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { ColLength, Dialect } from 'src/app/app.constants';
import IConv from 'src/app/model/conv';
import { IColMaxLength } from 'src/app/model/edit-table';
import IRule from 'src/app/model/rule';
import { ConversionService } from 'src/app/services/conversion/conversion.service';
import { DataService } from 'src/app/services/data/data.service';
import { SidenavService } from 'src/app/services/sidenav/sidenav.service';
import data from 'src/data';

@Component({
  selector: 'app-edit-column-max-length',
  templateUrl: './edit-column-max-length.component.html',
  styleUrls: ['./edit-column-max-length.component.scss']
})
export class EditColumnMaxLengthComponent implements OnInit {
  @Input() ruleNameValid: boolean = false
  @Input() ruleName: string = ''
  @Input() ruleType: string = ''
  @Output() resetRuleType: EventEmitter<any> = new EventEmitter<any>()
  editColMaxLengthForm: FormGroup
  ruleId: string = ''
  tableNames: string[] = []
  viewRuleData: any = []
  viewRuleFlag: boolean = false
  conv: IConv = {} as IConv
  spTypes: any = []
  hintlabel: string = ''

  constructor(private fb: FormBuilder, private data: DataService, private sidenav: SidenavService, private conversion: ConversionService) {
    this.editColMaxLengthForm = this.fb.group({
      tableName: ['', Validators.required],
      column: ['allColumn', Validators.required],
      spDataType: ['', Validators.required],
      maxColLength: ['', [Validators.required, Validators.pattern('([1-9][0-9]*|MAX)')]],
    })
  }

  ngOnInit(): void {
    this.data.conv.subscribe({
      next: (res: IConv) => {
        this.conv = res
        this.tableNames = Object.keys(res.SpSchema).map(
          (talbeId: string) => res.SpSchema[talbeId].Name
        )
        this.tableNames.push('All table')
        if (this.conv.SpDialect === Dialect.PostgreSQLDialect) {
          this.spTypes = [
            {
              name: 'VARCHAR',
              value: 'STRING',
            },
          ]
          this.hintlabel = 'Max ' + ColLength.StringMaxLength + ' for VARCHAR'
        } else {
          this.spTypes = [
            {
              name: 'STRING',
              value: 'STRING',
            },
            {
              name: 'BYTES',
              value: 'BYTES',
            },
          ]
          this.hintlabel = 'Max ' + ColLength.StringMaxLength + ' for STRING and ' + ColLength.ByteMaxLength + ' for BYTES'
        }
      },
    })

    this.sidenav.displayRuleFlag.subscribe((flag: boolean) => {
      this.viewRuleFlag = flag
      if (this.viewRuleFlag) {
        this.sidenav.ruleData.subscribe((data: IRule) => {
          this.viewRuleData = data
          if (this.viewRuleData) {
            this.ruleId = this.viewRuleData?.Id
            let tableName: string = this.conv.SpSchema[this.viewRuleData?.AssociatedObjects].Name
            this.editColMaxLengthForm.controls['tableName'].setValue(tableName)
            this.editColMaxLengthForm.controls['spDataType'].setValue(this.viewRuleData?.Data?.spDataType)
            this.editColMaxLengthForm.controls['maxColLength'].setValue(this.viewRuleData?.Data?.spColMaxLength)
            this.editColMaxLengthForm.disable()
          }
        })
      }
    })

  }

  formSubmit(): void {
    const ruleValue = this.editColMaxLengthForm.value
    if ((ruleValue.spDataType === 'STRING' || ruleValue.spDataType === 'VARCHAR') && ruleValue.spColMaxLength > ColLength.StringMaxLength) {
      ruleValue.spColMaxLength = ColLength.StorageMaxLength
    } else if (ruleValue.spDataType === 'BYTES' && ruleValue.spColMaxLength > ColLength.ByteMaxLength) {
      ruleValue.spColMaxLength = ColLength.StorageMaxLength
    }
    const data: IColMaxLength = {
      spDataType: ruleValue.spDataType,
      spColMaxLength: ruleValue.maxColLength
    }
    let tableId: string = this.conversion.getTableIdFromSpName(ruleValue.tableName, this.conv)
    let associatedObjects = tableId
    if (associatedObjects === '') {
      associatedObjects = 'All table'
    }
    let payload: IRule = {
      Name: this.ruleName,
      Type: 'edit_column_max_length',
      ObjectType: 'Table',
      AssociatedObjects: associatedObjects,
      Enabled: true,
      Data: data,
      Id: '',
    }

    this.data.applyRule(payload)
    this.resetRuleType.emit('')
    this.sidenav.closeSidenav()
  }

  deleteRule() {
    this.data.dropRule(this.ruleId)
    this.resetRuleType.emit('')
    this.sidenav.closeSidenav()
  }
}