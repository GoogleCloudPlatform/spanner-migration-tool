import { Component, OnInit } from '@angular/core';
import { FormControl, FormGroup, Validators } from '@angular/forms';
import { MatDialogRef } from '@angular/material/dialog';
import { Gcs } from 'src/app/app.constants';

@Component({
  selector: 'app-tune-gcs-form',
  templateUrl: './tune-gcs-form.component.html',
  styleUrls: ['./tune-gcs-form.component.scss'],
})
export class TuneGcsFormComponent implements OnInit {
  gcsForm: FormGroup
  ttlInDaysSet = false
  
  constructor(private dialofRef: MatDialogRef<TuneGcsFormComponent>) {
    this.gcsForm = new FormGroup({
      ttlInDays: new FormControl('',[Validators.required, Validators.pattern('^([1-9][0-9]*$)'), Validators.min(1), Validators.max(100000000)]),
    })
  }

  ngOnInit(): void {
  }

  updateGcsDetails() {
    let formValue = this.gcsForm.value
    if (this.ttlInDaysSet) {
      localStorage.setItem(Gcs.TtlInDays, formValue.ttlInDays)
      localStorage.setItem(Gcs.TtlInDaysSet, "true")
    } else {
      localStorage.setItem(Gcs.TtlInDays, "0")
      localStorage.setItem(Gcs.TtlInDaysSet, "false")
    }
    localStorage.setItem(Gcs.IsGcsConfigSet, "true")
    this.dialofRef.close()
  }
}
