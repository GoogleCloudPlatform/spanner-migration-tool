import { Component, Inject, OnInit } from '@angular/core';
import { FormControl, FormGroup, Validators } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { Dataflow } from 'src/app/app.constants';
import ISpannerConfig from 'src/app/model/spanner-config';

@Component({
  selector: 'app-dataflow-form',
  templateUrl: './dataflow-form.component.html',
  styleUrls: ['./dataflow-form.component.scss']
})
export class DataflowFormComponent implements OnInit {
  dataflowForm: FormGroup
  presetFlagsForm: FormGroup
  disablePresetFlags: boolean = true

  constructor(
    @Inject(MAT_DIALOG_DATA) public data: ISpannerConfig,
    private dialofRef: MatDialogRef<DataflowFormComponent>
  ) {
    this.dataflowForm = new FormGroup({
      network: new FormControl(''),
      subnetwork: new FormControl(''),
      numWorkers: new FormControl('1',[Validators.required, Validators.pattern('^[1-9][0-9]*$')]),
      maxWorkers: new FormControl('50', [Validators.required, Validators.pattern('^[1-9][0-9]*$')]),
      serviceAccountEmail: new FormControl(''),
      vpcHostProjectId: new FormControl(data.GCPProjectID, Validators.required),
      machineType: new FormControl(''),
      additionalUserLabels: new FormControl(''),
      kmsKeyName: new FormControl('', [Validators.pattern('^projects\\/[^\\n\\r]+\\/locations\\/[^\\n\\r]+\\/keyRings\\/[^\\n\\r]+\\/cryptoKeys\\/[^\\n\\r]+$')]),
    })
    this.presetFlagsForm = new FormGroup({
      dataflowProjectId: new FormControl(data.GCPProjectID),
      dataflowLocation: new FormControl(''),
      gcsTemplatePath: new FormControl('', [Validators.pattern('^gs:\\/\\/[^\\n\\r]+$')]),
    })
    this.presetFlagsForm.disable()
  }

  ngOnInit(): void {
  }

  updateDataflowDetails() {
    let formValue = this.dataflowForm.value
    localStorage.setItem(Dataflow.Network, formValue.network)
    localStorage.setItem(Dataflow.Subnetwork, formValue.subnetwork)
    localStorage.setItem(Dataflow.VpcHostProjectId, formValue.vpcHostProjectId)
    localStorage.setItem(Dataflow.MaxWorkers, formValue.maxWorkers)
    localStorage.setItem(Dataflow.NumWorkers, formValue.numWorkers)
    localStorage.setItem(Dataflow.ServiceAccountEmail, formValue.serviceAccountEmail)
    localStorage.setItem(Dataflow.MachineType, formValue.machineType)
    localStorage.setItem(Dataflow.AdditionalUserLabels, formValue.additionalUserLabels)
    localStorage.setItem(Dataflow.KmsKeyName, formValue.kmsKeyName)
    localStorage.setItem(Dataflow.ProjectId, this.presetFlagsForm.value.dataflowProjectId)
    localStorage.setItem(Dataflow.Location, this.presetFlagsForm.value.dataflowLocation)
    localStorage.setItem(Dataflow.GcsTemplatePath, this.presetFlagsForm.value.gcsTemplatePath)
    localStorage.setItem(Dataflow.IsDataflowConfigSet, "true")
    this.dialofRef.close()
  }

  enablePresetFlags(){
    this.disablePresetFlags = false
    this.presetFlagsForm.enable()
  }
}
