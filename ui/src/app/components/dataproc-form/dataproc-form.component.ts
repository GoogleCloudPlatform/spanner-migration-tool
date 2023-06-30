import { Component, OnInit } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { MatDialogRef } from '@angular/material/dialog';
import { Dataproc } from 'src/app/app.constants';

@Component({
  selector: 'app-dataproc-form',
  templateUrl: './dataproc-form.component.html',
  styleUrls: ['./dataproc-form.component.scss']
})
export class DataprocFormComponent implements OnInit {

  dataprocForm: FormGroup

  constructor(
    private formBuilder: FormBuilder,
    private dialofRef: MatDialogRef<DataprocFormComponent>
  ) {
    this.dataprocForm = this.formBuilder.group({
      subnetwork: ['', [Validators.required, Validators.pattern('^projects/[^/]+/regions/[^/]+/subnetworks/[^/]+')]],
      hostname: ['', {optional: true}],
      port: ['', Validators.pattern('^[0-9]+$')],
    })
  }

  ngOnInit(): void {
  }

  updateDataprocDetails() {
    let formValue = this.dataprocForm.value
    localStorage.setItem(Dataproc.Subnetwork, formValue.subnetwork)
    localStorage.setItem(Dataproc.Hostname, formValue.hostname)
    localStorage.setItem(Dataproc.Port, formValue.port)
    localStorage.setItem(Dataproc.IsDataprocConfigSet, "true")
    this.dialofRef.close()
  }

}
