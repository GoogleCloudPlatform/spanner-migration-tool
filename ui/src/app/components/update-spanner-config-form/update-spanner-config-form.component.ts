import { Component, Inject, OnInit } from '@angular/core'
import { FormControl, FormGroup } from '@angular/forms'
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service'
import ISpannerConfig from '../../model/SpannerConfig'
import { DataService } from 'src/app/services/data/data.service'

@Component({
  selector: 'app-update-spanner-config-form',
  templateUrl: './update-spanner-config-form.component.html',
  styleUrls: ['./update-spanner-config-form.component.scss'],
})
export class UpdateSpannerConfigFormComponent implements OnInit {
  errMessage: string = ''
  updateConfigFrom: FormGroup
  constructor(
    private fetch: FetchService,
    private snack: SnackbarService,
    private dataService: DataService,
    @Inject(MAT_DIALOG_DATA) public data: ISpannerConfig,
    private dialogRef: MatDialogRef<UpdateSpannerConfigFormComponent>
  ) {
    this.updateConfigFrom = new FormGroup({
      GCPProjectID: new FormControl(data.GCPProjectID),
      SpannerInstanceID: new FormControl(data.SpannerInstanceID),
    })
  }

  updateSpannerConfig() {
    let formValue = this.updateConfigFrom.value
    let payload: ISpannerConfig = {
      GCPProjectID: formValue.GCPProjectID,
      SpannerInstanceID: formValue.SpannerInstanceID,
    }

    console.log(payload)
    this.fetch.setSpannerConfig(payload).subscribe({
      next: (res: ISpannerConfig) => {
        this.snack.openSnackBar('Spanner Config updated successfully', 'close', 5000)
        this.dialogRef.close({ ...res })
        this.dataService.updateConfig(res)
      },
      error: (err: any) => {
        console.log(err)
        this.snack.openSnackBar(err.message, 'close', 5000)
      },
    })
  }

  ngOnInit(): void {}
}
