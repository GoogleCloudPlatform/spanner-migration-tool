import { Component, Input, OnInit } from '@angular/core'
import { FormArray, FormBuilder, FormControl, FormGroup, Validators } from '@angular/forms'
import IRuleContent from 'src/app/model/Rule'
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
  addGlobalDataTypeForm: FormGroup
  conversionType: Record<string, IConvSourceType[]> = {}
  sourceType: string[] = []
  destinationType: string[] = []
  constructor(
    private fb: FormBuilder,
    private data: DataService,
    private sidenavService: SidenavService
  ) {
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
  }

  closeSidenav(): void {
    this.sidenavService.closeSidenav()
  }
  formSubmit(): void {
    const ruleValue = this.addGlobalDataTypeForm.value
    const source = ruleValue.sourceType
    const payload: Record<string, string> = {}
    payload[source] = ruleValue.destinationType
    // const nextData: IRuleContent = {
    //   name: 'varchar to bytes',
    //   type: 'Global-data-type',
    //   objectType: ruleValue.objectType,
    //   associatedObject: 'All tables',
    //   enabled: true,
    // }
    this.data.updateGlobalType(payload)
    // this.data.addRule(nextData)
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
