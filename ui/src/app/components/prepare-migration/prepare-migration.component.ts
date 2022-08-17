import { Component, OnInit } from '@angular/core'
import { MatDialog } from '@angular/material/dialog'
import { TargetDetailsFormComponent } from '../target-details-form/target-details-form.component'
import { TargetDetailsService } from 'src/app/services/target-details/target-details.service'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service'
import ITargetDetails from 'src/app/model/target-details'
import { ISessionSummary } from 'src/app/model/conv'
import IMigrationDetails, { IProgress } from 'src/app/model/migrate'
import { InputType, MigrationModes, SourceDbNames } from 'src/app/app.constants'
import { interval, Observable, Subscription } from 'rxjs'
@Component({
  selector: 'app-prepare-migration',
  templateUrl: './prepare-migration.component.html',
  styleUrls: ['./prepare-migration.component.scss'],
})
export class PrepareMigrationComponent implements OnInit {
  displayedColumns = ['Title', 'Source', 'Destination']
  dataSource: any = []
  migrationModes: any = []
  subscription!: Subscription
  constructor(
    private dialog: MatDialog,
    private fetch: FetchService,
    private snack: SnackbarService,
    private targetDetailService: TargetDetailsService
  ) { }

  isTargetDetailSet: boolean = false
  isStreamingCfgSet: boolean = false
  isSchemaMigration: boolean = true
  isStreamingSupported: boolean = false
  isDisabled: boolean = false
  error: boolean = false
  selectedMigrationMode: string = MigrationModes.schemaOnly
  selectedMigrationType: string = 'bulk'
  errorMessage: string = ''
  progressMessage: string = ''
  progress: number = 0
  targetDetails: ITargetDetails = this.targetDetailService.getTargetDetails()

  ngOnInit(): void {
    this.fetch.getSourceDestinationSummary().subscribe({
      next: (res: ISessionSummary) => {
        this.dataSource = [
          { title: 'Database Type', source: res.DatabaseType, target: 'Spanner' },
          {
            title: 'Number of tables',
            source: res.SourceTableCount,
            target: res.SpannerTableCount,
          },
          {
            title: 'Number of indexes',
            source: res.SourceIndexCount,
            target: res.SpannerIndexCount,
          },
        ]
        if (res.ConnectionType == InputType.DumpFile) {
          this.migrationModes = [MigrationModes.schemaAndData]
          this.selectedMigrationMode = MigrationModes.schemaAndData
        } else if (res.ConnectionType == InputType.SessionFile) {
          this.migrationModes = [MigrationModes.schemaOnly]
        } else {
          this.migrationModes = [MigrationModes.schemaOnly, MigrationModes.dataOnly, MigrationModes.schemaAndData]
        }
        if (res.DatabaseType == SourceDbNames.MySQL.toLowerCase() || res.DatabaseType == SourceDbNames.Oracle.toLowerCase()) {
          this.isStreamingSupported = true
        }
      },
      error: (err: any) => {
        this.snack.openSnackBar(err.error, 'Close')
      },
    })
  }

  openTargetDetailsForm() {
    let dialogRef = this.dialog.open(TargetDetailsFormComponent, {
      width: '30vw',
      minWidth: '400px',
      maxWidth: '500px',
      data: this.selectedMigrationType == 'lowdt',
    })
    dialogRef.afterClosed().subscribe(() => {
      if (this.targetDetails.TargetDB != '') {
        this.isTargetDetailSet = true
      }
      if (this.targetDetails.StreamingConfig != '') {
        this.isStreamingCfgSet = true
      }
    })
  }

  migrate() {
    this.isDisabled = !this.isDisabled
    let payload: IMigrationDetails = {
      TargetDetails: this.targetDetailService.getTargetDetails(),
      MigrationType: this.selectedMigrationType,
      MigrationMode: this.selectedMigrationMode,
    }
    this.fetch.migrate(payload).subscribe({
      next: () => {
        this.snack.openSnackBar('Migration started successfully', 'Close', 5)
      },
      error: (err: any) => {
        this.snack.openSnackBar(err.error, 'Close')
      },
    })
    this.subscription = interval(5000).subscribe((x => {
      this.fetch.getProgress().subscribe({
        next: (res: IProgress) => {
          if (res.ErrorMessage == '') {
            this.progress = res.Progress
            this.progressMessage = res.Message
            if (this.progress == 100 && this.progressMessage.startsWith('Updating schema of database')) {
              this.subscription.unsubscribe();
              this.isDisabled = !this.isDisabled
            }

          } else {
            this.error = true;
            this.errorMessage = res.ErrorMessage;
            this.subscription.unsubscribe();
            this.isDisabled = !this.isDisabled
          }
        },
        error: (err: any) => {
          this.snack.openSnackBar(err.error, 'Close')
        },
      })
      console.log('called');
    }));

  }
  ngOnDestroy() {
    if (this.subscription) {
      this.subscription.unsubscribe();
    }

  }
}
