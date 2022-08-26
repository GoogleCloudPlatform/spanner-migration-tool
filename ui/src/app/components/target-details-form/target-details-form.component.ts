import { Component, Inject, OnInit } from '@angular/core'
import { FormBuilder, FormGroup, Validators } from '@angular/forms'
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog'
import ITargetDetails from 'src/app/model/target-details'
import { TargetDetailsService } from 'src/app/services/target-details/target-details.service'

@Component({
  selector: 'app-target-details-form',
  templateUrl: './target-details-form.component.html',
  styleUrls: ['./target-details-form.component.scss'],
})
export class TargetDetailsFormComponent implements OnInit {
  targetDetailsForm: FormGroup
  constructor(
    private formBuilder: FormBuilder,
    private targetDetailService: TargetDetailsService,
    private dialogRef: MatDialogRef<TargetDetailsFormComponent>,
    @Inject(MAT_DIALOG_DATA) public data: boolean
  ) {
    this.targetDetailsForm = this.formBuilder.group({
      targetDb: ['', Validators.required],
      dialect: ['', Validators.required],
      streamingConfig: ['', Validators.required],
    })
    if (!data) {
      this.targetDetailsForm.get('streamingConfig')?.disable()
    }
  }
  targetDetails: ITargetDetails = this.targetDetailService.getTargetDetails()

  ngOnInit(): void {
    this.targetDetailsForm.setValue({
      targetDb: this.targetDetails.TargetDB,
      dialect: this.targetDetails.Dialect,
      streamingConfig: this.targetDetails.StreamingConfig
    })
  }

  updateTargetDetails() {
    let formValue = this.targetDetailsForm.value
    let payload: ITargetDetails = {
      TargetDB: formValue.targetDb,
      Dialect: formValue.dialect,
      StreamingConfig: formValue.streamingConfig
    }
    this.targetDetailService.updateTargetDetails(payload)
    this.dialogRef.close()
  }
}