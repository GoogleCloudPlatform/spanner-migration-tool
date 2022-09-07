import { Component, OnInit } from '@angular/core'
import { MatDialog } from '@angular/material/dialog'
import { TargetDetailsFormComponent } from '../target-details-form/target-details-form.component'
import { TargetDetailsService } from 'src/app/services/target-details/target-details.service'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service'
import ITargetDetails from 'src/app/model/target-details'
import { ISessionSummary } from 'src/app/model/conv'
import IMigrationDetails, { IProgress } from 'src/app/model/migrate'
import { InputType, MigrationModes, MigrationTypes, SourceDbNames } from 'src/app/app.constants'
import { interval, Subscription } from 'rxjs'
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
  isStreamingSupported: boolean = false
  isButtonDisabled: boolean = false
  hasDataMigrationStarted: boolean = false
  hasDataMigrationCompleted: boolean = false
  hasSchemaMigrationStarted: boolean = false
  selectedMigrationMode: string = MigrationModes.schemaOnly
  connectionType: string = InputType.DirectConnect
  selectedMigrationType: string = MigrationTypes.bulkMigration
  errorMessage: string = ''
  schemaProgressMessage: string = 'Schema migration in progress...'
  dataProgressMessage: string = 'Data migration in progress...'
  dataMigrationProgress: number = 0
  schemaMigrationProgress: number = 0
  targetDetails: ITargetDetails = this.targetDetailService.getTargetDetails()

  ngOnInit(): void {
    this.fetch.getSourceDestinationSummary().subscribe({
      next: (res: ISessionSummary) => {
        this.connectionType = res.ConnectionType
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
          this.migrationModes = [MigrationModes.schemaOnly, MigrationModes.dataOnly, MigrationModes.schemaAndData]
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
      data: this.selectedMigrationType == MigrationTypes.lowDowntimeMigration,
    })
    dialogRef.afterClosed().subscribe(() => {
      if (this.targetDetails.TargetDB != '') {
        this.isTargetDetailSet = true
      }
    })
  }

  migrate() {
    this.resetValues()
    let payload: IMigrationDetails = {
      TargetDetails: this.targetDetailService.getTargetDetails(),
      MigrationType: this.selectedMigrationType,
      MigrationMode: this.selectedMigrationMode,
    }
    this.fetch.migrate(payload).subscribe({
      next: () => {
        if (this.selectedMigrationMode == MigrationModes.dataOnly) {
          this.hasDataMigrationStarted = true
        } else {
          this.hasSchemaMigrationStarted = true
        }
        this.snack.openSnackBar('Migration started successfully', 'Close', 5)
        this.subscribeMigrationProgress()
      },
      error: (err: any) => {
        this.snack.openSnackBar(err.error, 'Close')
        this.isButtonDisabled = !this.isButtonDisabled
        this.hasDataMigrationStarted = false
        this.hasSchemaMigrationStarted = false
        console.log("Error here")
      },
    })
  }

  subscribeMigrationProgress() {
    this.subscription = interval(5000).subscribe((x => {
      this.fetch.getProgress().subscribe({
        next: (res: IProgress) => {
          if (res.ErrorMessage == '') {
            // Checking for completion of schema migration
            if (res.Message.startsWith('Schema migration complete')) {
              this.schemaMigrationProgress = 100
              if (res.Progress == 100) {
                if (this.selectedMigrationMode == MigrationModes.schemaOnly) {
                  this.markMigrationComplete()
                }
              }
            }
            // Checking for data migration in progree
            else if (res.Message.startsWith('Writing data to Spanner')) {
              this.markSchemaMigrationComplete()
              this.dataMigrationProgress = res.Progress
              if (this.hasDataMigrationCompleted) {
                this.markMigrationComplete()
              }
              if (res.Progress == 100) {
                this.hasDataMigrationCompleted = true
              }
            }
            // Checking for foreign key update in progress
            else if (res.Message.startsWith('Updating schema of database')) {
              this.markSchemaMigrationComplete()
              this.dataMigrationProgress = 100
              if (res.Progress == 100) {
                this.markMigrationComplete()
              }
            }
          } else {
            this.errorMessage = res.ErrorMessage;
            this.subscription.unsubscribe();
            this.isButtonDisabled = !this.isButtonDisabled
            this.snack.openSnackBarWithoutTimeout(this.errorMessage, 'Close')
            this.schemaProgressMessage = "Schema migration cancelled!"
            this.dataProgressMessage = "Data migration cancelled!"
          }
        },
        error: (err: any) => {
          this.snack.openSnackBar(err.error, 'Close')
          this.isButtonDisabled = !this.isButtonDisabled
        },
      })
    }));
  }

  markSchemaMigrationComplete() {
    this.hasDataMigrationStarted = true
    this.schemaMigrationProgress = 100
    this.schemaProgressMessage = "Schema migration completed successfully!"
  }

  markMigrationComplete() {
    this.subscription.unsubscribe();
    this.isButtonDisabled = !this.isButtonDisabled
    this.dataProgressMessage = "Data migration completed successfully!"
    this.schemaProgressMessage = "Schema migration completed successfully!"
  }
  resetValues() {
    this.isButtonDisabled = !this.isButtonDisabled
    this.hasSchemaMigrationStarted = false
    this.hasDataMigrationStarted = false
    this.hasDataMigrationCompleted = false
    this.dataMigrationProgress = 0
    this.schemaMigrationProgress = 0
    this.schemaProgressMessage = "Schema creation in progress..."
    this.dataProgressMessage = "Data migration in progress..."
  }
  ngOnDestroy() {
    if (this.subscription) {
      this.subscription.unsubscribe();
    }

  }
}
