import { Component, OnInit } from '@angular/core'
import { FormBuilder, FormGroup, Validators } from '@angular/forms'
import { MatDialogRef} from '@angular/material/dialog'
import { MigrationDetails, TargetDetails } from 'src/app/app.constants'

@Component({
  selector: 'app-gcs-metadata-details-form',
  templateUrl: './gcs-metadata-details-form.component.html',
  styleUrls: ['./gcs-metadata-details-form.component.scss'],
})
export class GcsMetadataDetailsFormComponent implements OnInit {
  gcsMetadataDetailsForm: FormGroup

  constructor(
    private formBuilder: FormBuilder,
    private dialogRef: MatDialogRef<GcsMetadataDetailsFormComponent>,
  ) {
    this.gcsMetadataDetailsForm = this.formBuilder.group({
      gcsName: ['', [Validators.required, Validators.pattern('^(?!\.$)(?!\.\.$)(?!.*[\r\n])[^\r\n]{1,1024}$')]],
      gcsRootPath: [''],
    })

    this.gcsMetadataDetailsForm.setValue({
      gcsName: localStorage.getItem(TargetDetails.GcsMetadataName) || '',
      gcsRootPath: localStorage.getItem(TargetDetails.GcsMetadataRootPath) || ''
    })
  }

  ngOnInit(): void {}

  updateGcsPathMetadataDetails() {
    let formValue = this.gcsMetadataDetailsForm.value
    localStorage.setItem(TargetDetails.GcsMetadataName, formValue.gcsName || '')
    localStorage.setItem(TargetDetails.GcsMetadataRootPath, formValue.gcsRootPath || '')
    localStorage.setItem(MigrationDetails.IsGcsMetadataPathSet, "true")
    this.dialogRef.close()
  }
}
