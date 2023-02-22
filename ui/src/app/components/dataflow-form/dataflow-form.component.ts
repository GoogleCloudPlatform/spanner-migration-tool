import { Component, OnInit } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { MatDialogRef } from '@angular/material/dialog';
import { Dataflow } from 'src/app/app.constants';

@Component({
  selector: 'app-dataflow-form',
  templateUrl: './dataflow-form.component.html',
  styleUrls: ['./dataflow-form.component.scss']
})
export class DataflowFormComponent implements OnInit {

  dataflowForm: FormGroup

  constructor(
    private formBuilder: FormBuilder,
    private dialofRef: MatDialogRef<DataflowFormComponent>
  ) {
    this.dataflowForm = this.formBuilder.group({
      network: ['', Validators.required],
      subnetwork: ['', Validators.required],
    })
  }

  ngOnInit(): void {
  }

  updateDataflowDetails() {
    let formValue = this.dataflowForm.value
    localStorage.setItem(Dataflow.Network, formValue.network)
    localStorage.setItem(Dataflow.Subnetwork, formValue.subnetwork)
    localStorage.setItem(Dataflow.IsDataflowConfigSet, "true")
    this.dialofRef.close()
  }
}
