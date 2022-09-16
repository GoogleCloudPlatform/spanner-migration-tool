import { Component, OnInit } from '@angular/core'
import { MatDialog } from '@angular/material/dialog'
import { TargetDetailsFormComponent } from '../target-details-form/target-details-form.component'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service'
import ITargetDetails from 'src/app/model/target-details'
import { ISessionSummary } from 'src/app/model/conv'
import IMigrationDetails, { IProgress } from 'src/app/model/migrate'
import { InputType, MigrationDetails, MigrationModes, MigrationTypes, SourceDbNames, TargetDetails } from 'src/app/app.constants'
import { interval, Subscription } from 'rxjs'
import { DataService } from 'src/app/services/data/data.service'
import { ThisReceiver } from '@angular/compiler'
import { ConnectionProfileFormComponent } from '../connection-profile-form/connection-profile-form.component'
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
    private data: DataService
  ) { }

  isTargetDetailSet: boolean = false
  isStreamingSupported: boolean = false
  hasDataMigrationStarted: boolean = false
  hasDataMigrationCompleted: boolean = false
  hasSchemaMigrationStarted: boolean = false
  selectedMigrationMode: string = MigrationModes.schemaOnly
  connectionType: string = InputType.DirectConnect
  selectedMigrationType: string = MigrationTypes.bulkMigration
  isMigrationInProgress: boolean = false
  errorMessage: string = ''
  schemaProgressMessage: string = 'Schema migration in progress...'
  dataProgressMessage: string = 'Data migration in progress...'
  dataMigrationProgress: number = 0
  schemaMigrationProgress: number = 0

  targetDetails: ITargetDetails = {
    TargetDB: localStorage.getItem(TargetDetails.TargetDB) as string,
    Dialect: localStorage.getItem(TargetDetails.Dialect) as string,
    StreamingConfig: localStorage.getItem(TargetDetails.StreamingConfig) as string
  }

  ngOnInit(): void {
    this.initializeFromLocalStorage()
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

  initializeFromLocalStorage() {
    if (localStorage.getItem(MigrationDetails.MigrationMode) != null) {
      this.selectedMigrationMode = localStorage.getItem(MigrationDetails.MigrationMode) as string
    }
    if (localStorage.getItem(MigrationDetails.MigrationType) != null) {
      this.selectedMigrationType = localStorage.getItem(MigrationDetails.MigrationType) as string
    }
    if (localStorage.getItem(MigrationDetails.IsMigrationInProgress) != null) {
      this.isMigrationInProgress = (localStorage.getItem(MigrationDetails.IsMigrationInProgress) as string === 'true')
      this.subscribeMigrationProgress()
    }
    if (localStorage.getItem(MigrationDetails.IsTargetDetailSet) != null) {
      this.isTargetDetailSet = (localStorage.getItem(MigrationDetails.IsTargetDetailSet) as string === 'true')
    }
    if (localStorage.getItem(MigrationDetails.HasSchemaMigrationStarted) != null) {
      this.hasSchemaMigrationStarted = (localStorage.getItem(MigrationDetails.HasSchemaMigrationStarted) as string === 'true')
    }
    if (localStorage.getItem(MigrationDetails.HasDataMigrationStarted) != null) {
      this.hasDataMigrationStarted = (localStorage.getItem(MigrationDetails.HasDataMigrationStarted) as string === 'true')
    }
    if (localStorage.getItem(MigrationDetails.HasDataMigrationCompleted) != null) {
      this.hasDataMigrationCompleted = (localStorage.getItem(MigrationDetails.HasDataMigrationCompleted) as string === 'true')
    }
    if (localStorage.getItem(MigrationDetails.DataMigrationProgress) != null) {
      this.dataMigrationProgress = parseInt(localStorage.getItem(MigrationDetails.DataMigrationProgress) as string)
    }
    if (localStorage.getItem(MigrationDetails.SchemaMigrationProgress) != null) {
      this.schemaMigrationProgress = parseInt(localStorage.getItem(MigrationDetails.SchemaMigrationProgress) as string)
    }
    if (localStorage.getItem(MigrationDetails.DataProgressMessage) != null) {
      this.dataProgressMessage = localStorage.getItem(MigrationDetails.DataProgressMessage) as string
    }
    if (localStorage.getItem(MigrationDetails.SchemaProgressMessage) != null) {
      this.schemaProgressMessage = localStorage.getItem(MigrationDetails.SchemaProgressMessage) as string
    }
  }

  clearLocalStorage() {
    localStorage.removeItem(MigrationDetails.MigrationMode)
    localStorage.removeItem(MigrationDetails.MigrationType)
    localStorage.removeItem(MigrationDetails.IsTargetDetailSet)
    localStorage.removeItem(MigrationDetails.IsMigrationInProgress)
    localStorage.removeItem(MigrationDetails.HasSchemaMigrationStarted)
    localStorage.removeItem(MigrationDetails.HasDataMigrationStarted)
    localStorage.removeItem(MigrationDetails.HasDataMigrationCompleted)
    localStorage.removeItem(MigrationDetails.DataMigrationProgress)
    localStorage.removeItem(MigrationDetails.SchemaMigrationProgress)
    localStorage.removeItem(MigrationDetails.DataProgressMessage)
    localStorage.removeItem(MigrationDetails.SchemaProgressMessage)
  }
  openConnectionProfileForm(isSource: boolean) {
    let dialogRef = this.dialog.open(ConnectionProfileFormComponent, {
      width: '30vw',
      minWidth: '400px',
      maxWidth: '500px',
      data: isSource,
    })
    dialogRef.afterClosed().subscribe()
  }

  openTargetDetailsForm() {
    let dialogRef = this.dialog.open(TargetDetailsFormComponent, {
      width: '30vw',
      minWidth: '400px',
      maxWidth: '500px',
      data: this.selectedMigrationType == MigrationTypes.lowDowntimeMigration,
    })
    dialogRef.afterClosed().subscribe(() => {
      this.targetDetails = {
        TargetDB: localStorage.getItem(TargetDetails.TargetDB) as string,
        Dialect: localStorage.getItem(TargetDetails.Dialect) as string,
        StreamingConfig: localStorage.getItem(TargetDetails.StreamingConfig) as string
      }
      if (this.targetDetails.TargetDB != '' || (this.selectedMigrationType == MigrationTypes.lowDowntimeMigration && this.targetDetails.StreamingConfig != '')) {
        this.isTargetDetailSet = true
        localStorage.setItem(MigrationDetails.IsTargetDetailSet, this.isTargetDetailSet.toString())
      }
    })
  }

  migrate() {
this.resetValues()
    let payload: IMigrationDetails = {
      TargetDetails: this.targetDetails,
      MigrationType: this.selectedMigrationType,
      MigrationMode: this.selectedMigrationMode,
    }
    this.fetch.migrate(payload).subscribe({
      next: () => {
        if (this.selectedMigrationMode == MigrationModes.dataOnly) {
          this.hasDataMigrationStarted = true
          localStorage.setItem(MigrationDetails.HasDataMigrationStarted, this.hasDataMigrationStarted.toString())
        } else {
          this.hasSchemaMigrationStarted = true
          localStorage.setItem(MigrationDetails.HasSchemaMigrationStarted, this.hasSchemaMigrationStarted.toString())
        }
        this.snack.openSnackBar('Migration started successfully', 'Close', 5)
        this.subscribeMigrationProgress()
      },
      error: (err: any) => {
        this.snack.openSnackBar(err.error, 'Close')
        this.isMigrationInProgress = !this.isMigrationInProgress
        this.hasDataMigrationStarted = false
        this.hasSchemaMigrationStarted = false
        this.clearLocalStorage()
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
              localStorage.setItem(MigrationDetails.SchemaMigrationProgress, "100")
              this.schemaMigrationProgress = parseInt(localStorage.getItem(MigrationDetails.SchemaMigrationProgress) as string)
              if (this.selectedMigrationMode == MigrationModes.schemaOnly || this.selectedMigrationType == MigrationTypes.lowDowntimeMigration) {
                this.markMigrationComplete()
              }
            }
            // Checking for data migration in progree
            else if (res.Message.startsWith('Writing data to Spanner')) {
              this.markSchemaMigrationComplete()
              localStorage.setItem(MigrationDetails.DataMigrationProgress, res.Progress.toString())
              this.dataMigrationProgress = parseInt(localStorage.getItem(MigrationDetails.DataMigrationProgress) as string)
              if (this.hasDataMigrationCompleted) {
                this.markMigrationComplete()
              }
              if (res.Progress == 100) {
                localStorage.setItem(MigrationDetails.HasDataMigrationCompleted, "true")
                this.hasDataMigrationCompleted = localStorage.getItem(MigrationDetails.HasDataMigrationCompleted) as string === 'true'

              }
            }
            // Checking for foreign key update in progress
            else if (res.Message.startsWith('Updating schema of database')) {
              this.markSchemaMigrationComplete()
              this.dataMigrationProgress = 100
              localStorage.setItem(MigrationDetails.DataMigrationProgress, this.dataMigrationProgress.toString())
              if (res.Progress == 100) {
                this.markMigrationComplete()
              }
            }
          } else {
            this.errorMessage = res.ErrorMessage;
            this.subscription.unsubscribe();
            this.isMigrationInProgress = !this.isMigrationInProgress
            this.snack.openSnackBarWithoutTimeout(this.errorMessage, 'Close')
            this.schemaProgressMessage = "Schema migration cancelled!"
            this.dataProgressMessage = "Data migration cancelled!"
            this.clearLocalStorage()
          }
        },
        error: (err: any) => {
          this.snack.openSnackBar(err.error, 'Close')
          this.isMigrationInProgress = !this.isMigrationInProgress
          this.clearLocalStorage()
        },
      })
    }));
  }

  markSchemaMigrationComplete() {
    this.hasDataMigrationStarted = true
    this.schemaMigrationProgress = 100
    this.schemaProgressMessage = "Schema migration completed successfully!"
    localStorage.setItem(MigrationDetails.HasDataMigrationStarted, this.hasDataMigrationStarted.toString())
    localStorage.setItem(MigrationDetails.SchemaMigrationProgress, this.schemaMigrationProgress.toString())
    localStorage.setItem(MigrationDetails.SchemaProgressMessage, this.schemaProgressMessage)
  }

  markMigrationComplete() {
    this.subscription.unsubscribe();
    this.isMigrationInProgress = !this.isMigrationInProgress
    this.dataProgressMessage = "Data migration completed successfully!"
    this.schemaProgressMessage = "Schema migration completed successfully!"
    this.clearLocalStorage()
  }
  resetValues() {
    this.isMigrationInProgress = !this.isMigrationInProgress
    this.hasSchemaMigrationStarted = false
    this.hasDataMigrationStarted = false
    this.hasDataMigrationCompleted = false
    this.dataMigrationProgress = 0
    this.schemaMigrationProgress = 0
    this.schemaProgressMessage = "Schema migration in progress..."
    this.dataProgressMessage = "Data migration in progress..."
    this.initializeLocalStorage()
  }
  initializeLocalStorage() {
    localStorage.setItem(MigrationDetails.MigrationMode, this.selectedMigrationMode)
    localStorage.setItem(MigrationDetails.MigrationType, this.selectedMigrationType)
    localStorage.setItem(MigrationDetails.IsMigrationInProgress, this.isMigrationInProgress.toString())
    localStorage.setItem(MigrationDetails.HasSchemaMigrationStarted, this.hasSchemaMigrationStarted.toString())
    localStorage.setItem(MigrationDetails.HasDataMigrationStarted, this.hasDataMigrationStarted.toString())
    localStorage.setItem(MigrationDetails.HasDataMigrationCompleted, this.hasDataMigrationCompleted.toString())
    localStorage.setItem(MigrationDetails.DataMigrationProgress, this.dataMigrationProgress.toString())
    localStorage.setItem(MigrationDetails.SchemaMigrationProgress, this.schemaMigrationProgress.toString())
    localStorage.setItem(MigrationDetails.SchemaProgressMessage, this.schemaProgressMessage)
    localStorage.setItem(MigrationDetails.DataProgressMessage, this.dataProgressMessage)
    localStorage.setItem(MigrationDetails.IsTargetDetailSet, this.isTargetDetailSet.toString())
  }
  ngOnDestroy() {
  }
}
