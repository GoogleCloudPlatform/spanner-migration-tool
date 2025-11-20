import { Component, Inject, OnInit } from '@angular/core'
import { FormBuilder, FormGroup, Validators } from '@angular/forms'
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog'
import { MigrationDetails, TargetDetails } from 'src/app/app.constants'
import { ISpannerDetails } from 'src/app/model/conv'
@Component({
  selector: 'app-target-details-form',
  templateUrl: './target-details-form.component.html',
  styleUrls: ['./target-details-form.component.scss'],
})
export class TargetDetailsFormComponent implements OnInit {
  targetDetailsForm: FormGroup
  region: string = ''
  spannerInstance: string = ''
  dialect: string = ''

  timezones: { timezone: string }[] = Object.values(Intl.supportedValuesOf('timeZone')).map((timezone) => ({ timezone: timezone }))

  constructor(
    private formBuilder: FormBuilder,
    private dialogRef: MatDialogRef<TargetDetailsFormComponent>,
    @Inject(MAT_DIALOG_DATA) public data: ISpannerDetails
  ) {
    this.region = data.Region
    this.spannerInstance = data.Instance
    this.dialect = data.Dialect
    this.targetDetailsForm = this.formBuilder.group({
      targetDb: ['', [Validators.required,Validators.pattern('^[a-z][a-z0-9-_]{0,28}[a-z0-9]$')]],
      defaultTimezone: [''],
    })
    this.targetDetailsForm.setValue({
      targetDb: localStorage.getItem(TargetDetails.TargetDB),
      defaultTimezone: localStorage.getItem(TargetDetails.DefaultTimezone),
    })
  }

  ngOnInit(): void {
  }

  updateTargetDetails() {
    let formValue = this.targetDetailsForm.value
    localStorage.setItem(TargetDetails.TargetDB, formValue.targetDb)
    localStorage.setItem(TargetDetails.Dialect, formValue.dialect)
    if (formValue.defaultTimezone) {
      localStorage.setItem(TargetDetails.DefaultTimezone, formValue.defaultTimezone)
    } else{
      localStorage.removeItem(TargetDetails.DefaultTimezone)
    }
    localStorage.setItem(MigrationDetails.IsTargetDetailSet, "true")
    this.dialogRef.close()
  }

  customSearchFn(term: string, item: { timezone: string }) {
    term = term.toLocaleLowerCase()
    return item.timezone.toLocaleLowerCase().indexOf(term) > -1
  }
}
