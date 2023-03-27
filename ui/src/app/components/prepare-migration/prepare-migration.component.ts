import { Component, OnInit } from '@angular/core'
import { MatDialog } from '@angular/material/dialog'
import { TargetDetailsFormComponent } from '../target-details-form/target-details-form.component'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service'
import ITargetDetails from 'src/app/model/target-details'
import { ISessionSummary, ISpannerDetails } from 'src/app/model/conv'
import IMigrationDetails, { IGeneratedResources, IProgress, ISourceAndTargetDetails } from 'src/app/model/migrate'
import { Dataflow, InputType, MigrationDetails, MigrationModes, MigrationTypes, ProgressStatus, SourceDbNames, TargetDetails } from 'src/app/app.constants'
import { interval, Subscription } from 'rxjs'
import { DataService } from 'src/app/services/data/data.service'
import { ConnectionProfileFormComponent } from '../connection-profile-form/connection-profile-form.component'
import { SourceDetailsFormComponent } from '../source-details-form/source-details-form.component'
import { EndMigrationComponent } from '../end-migration/end-migration.component'
import { IDataflowConfig, ISetUpConnectionProfile } from 'src/app/model/profile'
import { DataflowFormComponent } from '../dataflow-form/dataflow-form.component'
import ISpannerConfig from 'src/app/model/spanner-config'
@Component({
  selector: 'app-prepare-migration',
  templateUrl: './prepare-migration.component.html',
  styleUrls: ['./prepare-migration.component.scss'],
})
export class PrepareMigrationComponent implements OnInit {
  displayedColumns = ['Title', 'Source', 'Destination']
  dataSource: any = []
  migrationModes: any = []
  migrationTypes: any = []
  subscription!: Subscription
  constructor(
    private dialog: MatDialog,
    private fetch: FetchService,
    private snack: SnackbarService,
    private data: DataService
  ) {}

  isSourceConnectionProfileSet: boolean = false
  isTargetConnectionProfileSet: boolean = false
  isDataflowConfigurationSet: boolean = false
  isSourceDetailsSet: boolean = false
  isTargetDetailSet: boolean = false
  isMigrationDetailSet: boolean = false
  isStreamingSupported: boolean = false
  hasDataMigrationStarted: boolean = false
  hasSchemaMigrationStarted: boolean = false
  hasForeignKeyUpdateStarted: boolean = false
  selectedMigrationMode: string = MigrationModes.schemaAndData
  connectionType: string = InputType.DirectConnect
  selectedMigrationType: string = MigrationTypes.lowDowntimeMigration
  isMigrationInProgress: boolean = false
  isLowDtMigrationRunning: boolean = false
  isResourceGenerated: boolean = false
  generatingResources: boolean = false
  errorMessage: string = ''
  schemaProgressMessage: string = 'Schema migration in progress...'
  dataProgressMessage: string = 'Data migration in progress...'
  foreignKeyProgressMessage: string = 'Foreign key update in progress...'
  dataMigrationProgress: number = 0
  schemaMigrationProgress: number = 0
  foreignKeyUpdateProgress: number = 0
  sourceDatabaseName: string = ''
  sourceDatabaseType: string = ''
  resourcesGenerated: IGeneratedResources = {
    DatabaseName: '',
    DatabaseUrl: '',
    BucketName: '',
    BucketUrl: '',
    DataStreamJobName: '',
    DataStreamJobUrl: '',
    DataflowJobName: '',
    DataflowJobUrl: '',
  }
  region: string = ''
  instance: string = ''
  dialect: string = ''
  nodeCount: number = 0
  processingUnits: number = 0

  targetDetails: ITargetDetails = {
    TargetDB: localStorage.getItem(TargetDetails.TargetDB) as string,
    SourceConnProfile: localStorage.getItem(TargetDetails.SourceConnProfile) as string,
    TargetConnProfile: localStorage.getItem(TargetDetails.TargetConnProfile) as string,
    ReplicationSlot: localStorage.getItem(TargetDetails.ReplicationSlot) as string,
    Publication: localStorage.getItem(TargetDetails.Publication) as string,
  }

  dataflowConfig: IDataflowConfig = {
    Network: localStorage.getItem(Dataflow.Network) as string,
    Subnetwork: localStorage.getItem(Dataflow.Subnetwork) as string,
    HostProjectId: localStorage.getItem(Dataflow.HostProjectId) as string
  }
  spannerConfig: ISpannerConfig = {
    GCPProjectID: '',
    SpannerInstanceID: '',
    IsMetadataDbCreated: false,
    IsConfigValid: false
  }

  refreshMigrationMode() {
    if (
      !(this.selectedMigrationMode === MigrationModes.schemaOnly) &&
      this.isStreamingSupported &&
      !(this.connectionType === InputType.DumpFile)
    ) {
      this.migrationTypes = [
        {
          name: 'Bulk Migration',
          value: MigrationTypes.bulkMigration,
        },
        {
          name: 'Minimal downtime Migration',
          value: MigrationTypes.lowDowntimeMigration,
        },
      ]
    } else {
      this.selectedMigrationType = MigrationTypes.bulkMigration
      this.migrationTypes = [
        {
          name: 'Bulk Migration',
          value: MigrationTypes.bulkMigration,
        },
      ]
    }
  }

  refreshPrerequisites() {
    this.isSourceConnectionProfileSet = false
    this.isTargetConnectionProfileSet = false
    this.isTargetDetailSet = false
    this.refreshMigrationMode()
  }

  ngOnInit(): void {
    this.initializeFromLocalStorage()
    this.data.config.subscribe((res: ISpannerConfig) => {
      this.spannerConfig = res
    })
    localStorage.setItem(Dataflow.HostProjectId, this.spannerConfig.GCPProjectID)
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
        this.sourceDatabaseType = res.DatabaseType
        this.region = res.Region
        this.instance = res.Instance
        this.dialect = res.Dialect
        this.processingUnits = res.ProcessingUnits
        this.nodeCount = res.NodeCount
        this.migrationTypes = [
          {
            name: 'Bulk Migration',
            value: MigrationTypes.bulkMigration,
          },
          {
            name: 'Minimal downtime Migration',
            value: MigrationTypes.lowDowntimeMigration,
          },
        ]
        if (this.connectionType == InputType.DumpFile) {
          this.selectedMigrationType = MigrationTypes.bulkMigration
          this.migrationTypes = [
            {
              name: 'Bulk Migration',
              value: MigrationTypes.bulkMigration,
            },
          ]
        }
        this.sourceDatabaseName = res.SourceDatabaseName
        this.migrationModes = [
          MigrationModes.schemaOnly,
          MigrationModes.dataOnly,
          MigrationModes.schemaAndData,
        ]
        if (
          res.DatabaseType == SourceDbNames.MySQL.toLowerCase() ||
          res.DatabaseType == SourceDbNames.Oracle.toLowerCase() ||
          res.DatabaseType == SourceDbNames.Postgres.toLowerCase()
        ) {
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
      this.isMigrationInProgress =
        (localStorage.getItem(MigrationDetails.IsMigrationInProgress) as string) === 'true'
      this.subscribeMigrationProgress()
    }
    if (localStorage.getItem(MigrationDetails.IsTargetDetailSet) != null) {
      this.isTargetDetailSet =
        (localStorage.getItem(MigrationDetails.IsTargetDetailSet) as string) === 'true'
    }
    if (localStorage.getItem(MigrationDetails.IsSourceConnectionProfileSet) != null) {
      this.isSourceConnectionProfileSet =
        (localStorage.getItem(MigrationDetails.IsSourceConnectionProfileSet) as string) === 'true'
    }
    if (localStorage.getItem(Dataflow.IsDataflowConfigSet) != null) {
      this.isDataflowConfigurationSet = (localStorage.getItem(Dataflow.IsDataflowConfigSet) as string === 'true')
    }
    if (localStorage.getItem(MigrationDetails.IsTargetConnectionProfileSet) != null) {
      this.isTargetConnectionProfileSet =
        (localStorage.getItem(MigrationDetails.IsTargetConnectionProfileSet) as string) === 'true'
    }
    if (localStorage.getItem(MigrationDetails.IsSourceDetailsSet) != null) {
      this.isSourceDetailsSet =
        (localStorage.getItem(MigrationDetails.IsSourceDetailsSet) as string) === 'true'
    }
    if (localStorage.getItem(MigrationDetails.IsMigrationDetailSet) != null) {
      this.isMigrationDetailSet =
        (localStorage.getItem(MigrationDetails.IsMigrationDetailSet) as string) === 'true'
    }
    if (localStorage.getItem(MigrationDetails.HasSchemaMigrationStarted) != null) {
      this.hasSchemaMigrationStarted =
        (localStorage.getItem(MigrationDetails.HasSchemaMigrationStarted) as string) === 'true'
    }
    if (localStorage.getItem(MigrationDetails.HasDataMigrationStarted) != null) {
      this.hasDataMigrationStarted =
        (localStorage.getItem(MigrationDetails.HasDataMigrationStarted) as string) === 'true'
    }
    if (localStorage.getItem(MigrationDetails.DataMigrationProgress) != null) {
      this.dataMigrationProgress = parseInt(
        localStorage.getItem(MigrationDetails.DataMigrationProgress) as string
      )
    }
    if (localStorage.getItem(MigrationDetails.SchemaMigrationProgress) != null) {
      this.schemaMigrationProgress = parseInt(
        localStorage.getItem(MigrationDetails.SchemaMigrationProgress) as string
      )
    }
    if (localStorage.getItem(MigrationDetails.DataProgressMessage) != null) {
      this.dataProgressMessage = localStorage.getItem(
        MigrationDetails.DataProgressMessage
      ) as string
    }
    if (localStorage.getItem(MigrationDetails.SchemaProgressMessage) != null) {
      this.schemaProgressMessage = localStorage.getItem(
        MigrationDetails.SchemaProgressMessage
      ) as string
    }
    if (localStorage.getItem(MigrationDetails.ForeignKeyProgressMessage) != null) {
      this.foreignKeyProgressMessage = localStorage.getItem(
        MigrationDetails.ForeignKeyProgressMessage
      ) as string
    }
    if (localStorage.getItem(MigrationDetails.ForeignKeyUpdateProgress) != null) {
      this.foreignKeyUpdateProgress = parseInt(
        localStorage.getItem(MigrationDetails.ForeignKeyUpdateProgress) as string
      )
    }
    if (localStorage.getItem(MigrationDetails.HasForeignKeyUpdateStarted) != null) {
      this.hasForeignKeyUpdateStarted =
        (localStorage.getItem(MigrationDetails.HasForeignKeyUpdateStarted) as string) === 'true'
    }
    if (localStorage.getItem(MigrationDetails.GeneratingResources) != null) {
      this.generatingResources =
        (localStorage.getItem(MigrationDetails.GeneratingResources) as string) === 'true'
    }
  }

  clearLocalStorage() {
    localStorage.removeItem(MigrationDetails.MigrationMode)
    localStorage.removeItem(MigrationDetails.MigrationType)
    localStorage.removeItem(MigrationDetails.IsTargetDetailSet)
    localStorage.removeItem(MigrationDetails.IsSourceConnectionProfileSet)
    localStorage.removeItem(MigrationDetails.IsTargetConnectionProfileSet)
    localStorage.removeItem(MigrationDetails.IsSourceDetailsSet)
    localStorage.removeItem(Dataflow.IsDataflowConfigSet)
    localStorage.removeItem(Dataflow.Network)
    localStorage.removeItem(Dataflow.Subnetwork)
    localStorage.removeItem(Dataflow.HostProjectId)
    localStorage.removeItem(MigrationDetails.IsMigrationInProgress)
    localStorage.removeItem(MigrationDetails.HasSchemaMigrationStarted)
    localStorage.removeItem(MigrationDetails.HasDataMigrationStarted)
    localStorage.removeItem(MigrationDetails.DataMigrationProgress)
    localStorage.removeItem(MigrationDetails.SchemaMigrationProgress)
    localStorage.removeItem(MigrationDetails.DataProgressMessage)
    localStorage.removeItem(MigrationDetails.SchemaProgressMessage)
    localStorage.removeItem(MigrationDetails.ForeignKeyProgressMessage)
    localStorage.removeItem(MigrationDetails.ForeignKeyUpdateProgress)
    localStorage.removeItem(MigrationDetails.HasForeignKeyUpdateStarted)
    localStorage.removeItem(MigrationDetails.GeneratingResources)
  }
  openConnectionProfileForm(isSource: boolean) {
    let payload: ISetUpConnectionProfile = {
      IsSource: isSource,
      SourceDatabaseType: this.sourceDatabaseType,
    }
    let dialogRef = this.dialog.open(ConnectionProfileFormComponent, {
      width: '30vw',
      minWidth: '400px',
      maxWidth: '500px',
      data: payload,
    })
    dialogRef.afterClosed().subscribe(() => {
      this.targetDetails = {
        TargetDB: localStorage.getItem(TargetDetails.TargetDB) as string,
        SourceConnProfile: localStorage.getItem(TargetDetails.SourceConnProfile) as string,
        TargetConnProfile: localStorage.getItem(TargetDetails.TargetConnProfile) as string,
        ReplicationSlot: localStorage.getItem(TargetDetails.ReplicationSlot) as string,
        Publication: localStorage.getItem(TargetDetails.Publication) as string,
      }
      this.isSourceConnectionProfileSet =
        (localStorage.getItem(MigrationDetails.IsSourceConnectionProfileSet) as string) === 'true'
      this.isTargetConnectionProfileSet =
        (localStorage.getItem(MigrationDetails.IsTargetConnectionProfileSet) as string) === 'true'
      if (
        this.isTargetDetailSet &&
        this.isSourceConnectionProfileSet &&
        this.isTargetConnectionProfileSet
      ) {
        localStorage.setItem(MigrationDetails.IsMigrationDetailSet, 'true')
        this.isMigrationDetailSet = true
      }
    })
  }

  openDataflowForm() {
    let dialogRef = this.dialog.open(DataflowFormComponent, {
      width: '30vw',
      minWidth: '400px',
      maxWidth: '500px',
      data: this.spannerConfig,
    })
    dialogRef.afterClosed().subscribe(() => {
      this.dataflowConfig = {
        Network: localStorage.getItem(Dataflow.Network) as string,
        Subnetwork: localStorage.getItem(Dataflow.Subnetwork) as string,
        HostProjectId: localStorage.getItem(Dataflow.HostProjectId) as string
      }
      this.isDataflowConfigurationSet = localStorage.getItem(Dataflow.IsDataflowConfigSet) as string === 'true'
    }
    )
  }

  endMigration() {
    let payload: ISourceAndTargetDetails = {
      SpannerDatabaseName: this.resourcesGenerated.DatabaseName,
      SpannerDatabaseUrl: this.resourcesGenerated.DatabaseUrl,
      SourceDatabaseType: this.sourceDatabaseType,
      SourceDatabaseName: this.sourceDatabaseName,
    }
    let dialogRef = this.dialog.open(EndMigrationComponent, {
      width: '30vw',
      minWidth: '400px',
      maxWidth: '500px',
      data: payload,
    })
    dialogRef.afterClosed().subscribe()
  }

  openSourceDetailsForm() {
    let dialogRef = this.dialog.open(SourceDetailsFormComponent, {
      width: '30vw',
      minWidth: '400px',
      maxWidth: '500px',
      data: this.sourceDatabaseType,
    })
    dialogRef.afterClosed().subscribe(() => {
      this.isSourceDetailsSet =
        (localStorage.getItem(MigrationDetails.IsSourceDetailsSet) as string) === 'true'
    })
  }

  openTargetDetailsForm() {
    let spannerDetails: ISpannerDetails = {
      Region: this.region,
      Instance: this.instance,
      Dialect: this.dialect,
    }
    let dialogRef = this.dialog.open(TargetDetailsFormComponent, {
      width: '30vw',
      minWidth: '400px',
      maxWidth: '500px',
      data: spannerDetails,
    })
    dialogRef.afterClosed().subscribe(() => {
      this.targetDetails = {
        TargetDB: localStorage.getItem(TargetDetails.TargetDB) as string,
        SourceConnProfile: localStorage.getItem(TargetDetails.SourceConnProfile) as string,
        TargetConnProfile: localStorage.getItem(TargetDetails.TargetConnProfile) as string,
        ReplicationSlot: localStorage.getItem(TargetDetails.ReplicationSlot) as string,
        Publication: localStorage.getItem(TargetDetails.Publication) as string,
      }
      this.isTargetDetailSet =
        (localStorage.getItem(MigrationDetails.IsTargetDetailSet) as string) === 'true'
      if (
        this.isSourceDetailsSet &&
        this.isTargetDetailSet &&
        this.connectionType === InputType.SessionFile &&
        this.selectedMigrationMode !== MigrationModes.schemaOnly
      ) {
        localStorage.setItem(MigrationDetails.IsMigrationDetailSet, 'true')
        this.isMigrationDetailSet = true
      } else if (
        this.isTargetDetailSet &&
        this.selectedMigrationType == MigrationTypes.bulkMigration &&
        this.connectionType !== InputType.SessionFile
      ) {
        localStorage.setItem(MigrationDetails.IsMigrationDetailSet, 'true')
        this.isMigrationDetailSet = true
      } else if (
        this.isTargetDetailSet &&
        this.selectedMigrationType == MigrationTypes.bulkMigration &&
        this.connectionType === InputType.SessionFile &&
        this.selectedMigrationMode === MigrationModes.schemaOnly
      ) {
        localStorage.setItem(MigrationDetails.IsMigrationDetailSet, 'true')
        this.isMigrationDetailSet = true
      }
    })
  }

  migrate() {
    this.resetValues()
    let payload: IMigrationDetails = {
      TargetDetails: this.targetDetails,
      DataflowConfig: this.dataflowConfig,
      MigrationType: this.selectedMigrationType,
      MigrationMode: this.selectedMigrationMode,
    }
    this.fetch.migrate(payload).subscribe({
      next: () => {
        if (this.selectedMigrationMode == MigrationModes.dataOnly) {
          if (this.selectedMigrationType == MigrationTypes.bulkMigration) {
            this.hasDataMigrationStarted = true
            localStorage.setItem(
              MigrationDetails.HasDataMigrationStarted,
              this.hasDataMigrationStarted.toString()
            )
          } else {
            this.generatingResources = true
            localStorage.setItem(
              MigrationDetails.GeneratingResources,
              this.generatingResources.toString()
            )
            this.snack.openSnackBar('Setting up dataflow and datastream jobs', 'Close')
          }
        } else {
          this.hasSchemaMigrationStarted = true
          localStorage.setItem(
            MigrationDetails.HasSchemaMigrationStarted,
            this.hasSchemaMigrationStarted.toString()
          )
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
    var displayStreamingMsg = false
    this.subscription = interval(5000).subscribe((x) => {
      this.fetch.getProgress().subscribe({
        next: (res: IProgress) => {
          if (res.ErrorMessage == '') {
            // Checking for completion of schema migration
            if (res.ProgressStatus == ProgressStatus.SchemaMigrationComplete) {
              localStorage.setItem(MigrationDetails.SchemaMigrationProgress, '100')
              this.schemaMigrationProgress = parseInt(
                localStorage.getItem(MigrationDetails.SchemaMigrationProgress) as string
              )
              if (this.selectedMigrationMode == MigrationModes.schemaOnly) {
                this.markMigrationComplete()
              } else if (this.selectedMigrationType == MigrationTypes.lowDowntimeMigration) {
                this.markSchemaMigrationComplete()
                this.generatingResources = true
                localStorage.setItem(
                  MigrationDetails.GeneratingResources,
                  this.generatingResources.toString()
                )
                if (!displayStreamingMsg) {
                  this.snack.openSnackBar('Setting up dataflow and datastream jobs', 'Close')
                  displayStreamingMsg = true
                }
              } else {
                this.markSchemaMigrationComplete()
                this.hasDataMigrationStarted = true
                localStorage.setItem(
                  MigrationDetails.HasDataMigrationStarted,
                  this.hasDataMigrationStarted.toString()
                )
              }
            } else if (res.ProgressStatus == ProgressStatus.DataMigrationComplete) {
              if (this.selectedMigrationType != MigrationTypes.lowDowntimeMigration) {
                this.hasDataMigrationStarted = true
                localStorage.setItem(
                  MigrationDetails.HasDataMigrationStarted,
                  this.hasDataMigrationStarted.toString()
                )
              }
              this.generatingResources = false
              localStorage.setItem(
                MigrationDetails.GeneratingResources,
                this.generatingResources.toString()
              )
              this.markMigrationComplete()
            }
            // Checking for data migration in progress
            else if (res.ProgressStatus == ProgressStatus.DataWriteInProgress) {
              this.markSchemaMigrationComplete()
              this.hasDataMigrationStarted = true
              localStorage.setItem(
                MigrationDetails.HasDataMigrationStarted,
                this.hasDataMigrationStarted.toString()
              )
              localStorage.setItem(MigrationDetails.DataMigrationProgress, res.Progress.toString())
              this.dataMigrationProgress = parseInt(
                localStorage.getItem(MigrationDetails.DataMigrationProgress) as string
              )
            } else if (res.ProgressStatus == ProgressStatus.ForeignKeyUpdateComplete) {
              this.markMigrationComplete()
            }
            // Checking for foreign key update in progress
            else if (res.ProgressStatus == ProgressStatus.ForeignKeyUpdateInProgress) {
              this.markSchemaMigrationComplete()
              if (this.selectedMigrationType == MigrationTypes.bulkMigration) {
                this.hasDataMigrationStarted = true
                localStorage.setItem(
                  MigrationDetails.HasDataMigrationStarted,
                  this.hasDataMigrationStarted.toString()
                )
              }
              this.markForeignKeyUpdateInitiation()
              this.dataMigrationProgress = 100
              localStorage.setItem(
                MigrationDetails.DataMigrationProgress,
                this.dataMigrationProgress.toString()
              )
              localStorage.setItem(
                MigrationDetails.ForeignKeyUpdateProgress,
                res.Progress.toString()
              )
              this.foreignKeyUpdateProgress = parseInt(
                localStorage.getItem(MigrationDetails.ForeignKeyUpdateProgress) as string
              )
              this.generatingResources = false
              localStorage.setItem(
                MigrationDetails.GeneratingResources,
                this.generatingResources.toString()
              )
              this.fetchGeneratedResources()
            }
          } else {
            this.errorMessage = res.ErrorMessage
            this.subscription.unsubscribe()
            this.isMigrationInProgress = !this.isMigrationInProgress
            this.snack.openSnackBarWithoutTimeout(this.errorMessage, 'Close')
            this.schemaProgressMessage = 'Schema migration cancelled!'
            this.dataProgressMessage = 'Data migration cancelled!'
            this.foreignKeyProgressMessage = 'Foreign key update cancelled!'
            this.generatingResources = false
            this.isLowDtMigrationRunning = false
            this.clearLocalStorage()
          }
        },
        error: (err: any) => {
          this.snack.openSnackBar(err.error, 'Close')
          this.isMigrationInProgress = !this.isMigrationInProgress
          this.clearLocalStorage()
        },
      })
    })
  }

  markForeignKeyUpdateInitiation() {
    this.dataMigrationProgress = 100
    this.dataProgressMessage = 'Data migration completed successfully!'
    localStorage.setItem(
      MigrationDetails.DataMigrationProgress,
      this.dataMigrationProgress.toString()
    )
    localStorage.setItem(
      MigrationDetails.DataMigrationProgress,
      this.dataMigrationProgress.toString()
    )
    this.hasForeignKeyUpdateStarted = true
    this.foreignKeyUpdateProgress = parseInt(
      localStorage.getItem(MigrationDetails.ForeignKeyUpdateProgress) as string
    )
  }
  markSchemaMigrationComplete() {
    this.schemaMigrationProgress = 100
    this.schemaProgressMessage = 'Schema migration completed successfully!'
    localStorage.setItem(
      MigrationDetails.SchemaMigrationProgress,
      this.schemaMigrationProgress.toString()
    )
    localStorage.setItem(MigrationDetails.SchemaProgressMessage, this.schemaProgressMessage)
  }

  fetchGeneratedResources() {
    this.fetch.getGeneratedResources().subscribe({
      next: (res: IGeneratedResources) => {
        this.isResourceGenerated = true
        this.resourcesGenerated = res
      },
      error: (err: any) => {
        this.snack.openSnackBar(err.error, 'Close')
      },
    })
    if (this.selectedMigrationType === MigrationTypes.lowDowntimeMigration) {
      this.isLowDtMigrationRunning = true
    }
  }

  markMigrationComplete() {
    this.subscription.unsubscribe()
    this.isMigrationInProgress = !this.isMigrationInProgress
    this.dataProgressMessage = 'Data migration completed successfully!'
    this.schemaProgressMessage = 'Schema migration completed successfully!'
    this.schemaMigrationProgress = 100
    this.dataMigrationProgress = 100
    this.foreignKeyUpdateProgress = 100
    this.foreignKeyProgressMessage = 'Foreign key updated successfully!'
    this.fetchGeneratedResources()
    this.clearLocalStorage()
    this.refreshPrerequisites()
  }
  resetValues() {
    this.isMigrationInProgress = !this.isMigrationInProgress
    this.hasSchemaMigrationStarted = false
    this.hasDataMigrationStarted = false
    this.generatingResources = false
    this.dataMigrationProgress = 0
    this.schemaMigrationProgress = 0
    this.schemaProgressMessage = 'Schema migration in progress...'
    this.dataProgressMessage = 'Data migration in progress...'
    this.isResourceGenerated = false
    this.hasForeignKeyUpdateStarted = false
    this.foreignKeyUpdateProgress = 100
    this.foreignKeyProgressMessage = 'Foreign key update in progress...'
    this.resourcesGenerated = {
      DatabaseName: '',
      DatabaseUrl: '',
      BucketName: '',
      BucketUrl: '',
      DataStreamJobName: '',
      DataStreamJobUrl: '',
      DataflowJobName: '',
      DataflowJobUrl: '',
    }
    this.initializeLocalStorage()
  }
  initializeLocalStorage() {
    localStorage.setItem(MigrationDetails.MigrationMode, this.selectedMigrationMode)
    localStorage.setItem(MigrationDetails.MigrationType, this.selectedMigrationType)
    localStorage.setItem(
      MigrationDetails.IsMigrationInProgress,
      this.isMigrationInProgress.toString()
    )
    localStorage.setItem(
      MigrationDetails.HasSchemaMigrationStarted,
      this.hasSchemaMigrationStarted.toString()
    )
    localStorage.setItem(
      MigrationDetails.HasDataMigrationStarted,
      this.hasDataMigrationStarted.toString()
    )
    localStorage.setItem(
      MigrationDetails.HasForeignKeyUpdateStarted,
      this.hasForeignKeyUpdateStarted.toString()
    )
    localStorage.setItem(
      MigrationDetails.DataMigrationProgress,
      this.dataMigrationProgress.toString()
    )
    localStorage.setItem(
      MigrationDetails.SchemaMigrationProgress,
      this.schemaMigrationProgress.toString()
    )
    localStorage.setItem(
      MigrationDetails.ForeignKeyUpdateProgress,
      this.foreignKeyUpdateProgress.toString()
    )
    localStorage.setItem(MigrationDetails.SchemaProgressMessage, this.schemaProgressMessage)
    localStorage.setItem(MigrationDetails.DataProgressMessage, this.dataProgressMessage)
    localStorage.setItem(MigrationDetails.ForeignKeyProgressMessage, this.foreignKeyProgressMessage)
    localStorage.setItem(MigrationDetails.IsTargetDetailSet, this.isTargetDetailSet.toString())
    localStorage.setItem(MigrationDetails.GeneratingResources, this.generatingResources.toString())
  }
  ngOnDestroy() {}
}
