import { Component, OnInit } from '@angular/core'
import { FormBuilder, FormGroup, Validators } from '@angular/forms'
import { MatDialogRef } from '@angular/material/dialog'
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
    private fb: FormBuilder,
    private targetDetailService: TargetDetailsService,
    private dialogRef: MatDialogRef<TargetDetailsFormComponent>
  ) {
    this.targetDetailsForm = this.fb.group({
      targetDb: ['', Validators.required],
      dialect: ['',Validators.required],
      streamingConfig: [''],
    })
  }

  ngOnInit(): void {}

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