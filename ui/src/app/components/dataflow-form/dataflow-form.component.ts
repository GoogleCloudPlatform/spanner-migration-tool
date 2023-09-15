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
      hostProjectId: new FormControl(data.GCPProjectID, Validators.required),
    })
  }

  ngOnInit(): void {
  }

  updateDataflowDetails() {
    let formValue = this.dataflowForm.value
    localStorage.setItem(Dataflow.Network, formValue.network)
    localStorage.setItem(Dataflow.Subnetwork, formValue.subnetwork)
    localStorage.setItem(Dataflow.HostProjectId, formValue.hostProjectId)
    localStorage.setItem(Dataflow.MaxWorkers, formValue.maxWorkers)
    localStorage.setItem(Dataflow.NumWorkers, formValue.numWorkers)
    localStorage.setItem(Dataflow.ServiceAccountEmail, formValue.serviceAccountEmail)
    localStorage.setItem(Dataflow.IsDataflowConfigSet, "true")
    this.dialofRef.close()
  }
}
