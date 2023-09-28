import { Component, Inject, OnInit } from '@angular/core';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { ISourceAndTargetDetails } from 'src/app/model/migrate';
import { FetchService } from 'src/app/services/fetch/fetch.service';
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service';

@Component({
  selector: 'app-end-migration',
  templateUrl: './end-migration.component.html',
  styleUrls: ['./end-migration.component.scss']
})
export class EndMigrationComponent implements OnInit {

  sourceAndTargetDetails: ISourceAndTargetDetails = {
    SpannerDatabaseName: '',
    SpannerDatabaseUrl: '',
    SourceDatabaseName: '',
    SourceDatabaseType: ''
  }
  cleaningUp: boolean = false

  constructor(
    @Inject(MAT_DIALOG_DATA) public data: ISourceAndTargetDetails,
    private fetch: FetchService,
    private snack: SnackbarService,
    private dialogRef: MatDialogRef<EndMigrationComponent>,
  ) {
    this.sourceAndTargetDetails = {
      SourceDatabaseName: data.SourceDatabaseName,
      SourceDatabaseType: data.SourceDatabaseType,
      SpannerDatabaseName: data.SpannerDatabaseName,
      SpannerDatabaseUrl: data.SpannerDatabaseUrl
    }
  }

  ngOnInit(): void {
  }

  cleanUpJobs() {
    this.cleaningUp = true
    this.fetch.cleanUpStreamingJobs().subscribe({
      next: () => {
        this.cleaningUp = false
        this.snack.openSnackBar('Dataflow and datastream jobs will be cleaned up', 'Close')
        this.dialogRef.close()
      },
      error: (err: any) => {
        this.cleaningUp = false
        this.snack.openSnackBar(err.error, 'Close')
      }
    })
    
  }
}
