import { Component, Inject, OnInit } from '@angular/core'
import { FormBuilder, FormGroup, Validators } from '@angular/forms'
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog'
import { TargetDetails } from 'src/app/app.constants'
@Component({
  selector: 'app-target-details-form',
  templateUrl: './target-details-form.component.html',
  styleUrls: ['./target-details-form.component.scss'],
})
export class TargetDetailsFormComponent implements OnInit {
  targetDetailsForm: FormGroup
  regionList = ['us-central1']
  selectedRegion: string = 'us-central1'
  constructor(
    private formBuilder: FormBuilder,
    private dialogRef: MatDialogRef<TargetDetailsFormComponent>,
    @Inject(MAT_DIALOG_DATA) public data: boolean
  ) {
    this.targetDetailsForm = this.formBuilder.group({
      targetDb: ['', Validators.required],
      dialect: ['', Validators.required],
      region: ['', Validators.required],
    })
    if (!data) {
      this.targetDetailsForm.get('region')?.disable()
      localStorage.setItem(TargetDetails.Region, "")
    }
    this.targetDetailsForm.setValue({
      targetDb: localStorage.getItem(TargetDetails.TargetDB),
      dialect: localStorage.getItem(TargetDetails.Dialect),
      region: localStorage.getItem(TargetDetails.Region)
    })
  }

  ngOnInit(): void {
  }

  updateTargetDetails() {
    let formValue = this.targetDetailsForm.value
    localStorage.setItem(TargetDetails.TargetDB, formValue.targetDb)
    localStorage.setItem(TargetDetails.Dialect, formValue.dialect)
    if (formValue.region !== undefined) {
      localStorage.setItem(TargetDetails.Region, formValue.region)
    }
    this.dialogRef.close()
  }
}