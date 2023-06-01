import { Component, Inject, OnInit } from '@angular/core';
import { FormControl, FormGroup, Validators } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { error } from 'console';
import { Dataflow } from 'src/app/app.constants';
import { IDataflowConfig } from 'src/app/model/profile';
import ISpannerConfig from 'src/app/model/spanner-config';
import { FetchService } from 'src/app/services/fetch/fetch.service';

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
      network: new FormControl('', Validators.required),
      subnetwork: new FormControl('', Validators.required),
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
    localStorage.setItem(Dataflow.IsDataflowConfigSet, "true")
    this.dialofRef.close()
  }
}
