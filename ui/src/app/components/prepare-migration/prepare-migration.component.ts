import { Component, OnInit, ViewChild } from '@angular/core'
import { MatDialog } from '@angular/material/dialog'
import { TargetDetailsFormComponent } from '../target-details-form/target-details-form.component'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service'
import ITargetDetails from 'src/app/model/target-details'
import IConv, { ISessionSummary, ISpannerDetails } from 'src/app/model/conv'
import IMigrationDetails, { IGeneratedResources, IProgress, ISourceAndTargetDetails, ResourceDetails } from 'src/app/model/migrate'
import { Gcs, InputType, MigrationDetails, MigrationModes, MigrationTypes, ProgressStatus, SourceDbNames, TargetDetails, dialogDefault } from 'src/app/app.constants'
import { interval, Subscription } from 'rxjs'
import { DataService } from 'src/app/services/data/data.service'
import { SourceDetailsFormComponent } from '../source-details-form/source-details-form.component'
import { IGcsConfig, IMigrationProfile, ISetUpConnectionProfile } from 'src/app/model/profile'
import ISpannerConfig from 'src/app/model/spanner-config'
import { ShardedBulkSourceDetailsFormComponent } from '../sharded-bulk-source-details-form/sharded-bulk-source-details-form.component'
import { IShardSessionDetails } from 'src/app/model/db-config'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'
import { downloadSession, downloadOverrides } from 'src/app/utils/utils'
import {MatPaginator} from '@angular/material/paginator';
import { MatTableDataSource } from '@angular/material/table'
import { GcsMetadataDetailsFormComponent } from '../gcs-metadata-details-form/gcs-metadata-details-form.component'
@Component({
  selector: 'app-prepare-migration',
  templateUrl: './prepare-migration.component.html',
  styleUrls: ['./prepare-migration.component.scss'],
})
export class PrepareMigrationComponent implements OnInit {
  conv!: IConv
  convObj!: Subscription
  displayedColumns = ['Title', 'Source', 'Destination']
  dataSource: any = []
  migrationModes: any = []
  migrationTypes: any = []
  subscription!: Subscription
  constructor(
    private dialog: MatDialog,
    private fetch: FetchService,
    private snack: SnackbarService,
    private data: DataService,
    private sidenav: SidenavService,
  ) { }

  isSourceConnectionProfileSet: boolean = false
  isTargetConnectionProfileSet: boolean = false
  isGcsConfigurationSet: boolean = false
  isSourceDetailsSet: boolean = false
  isTargetDetailSet: boolean = false
  isForeignKeySkipped: boolean = false
  isMigrationDetailSet: boolean = false
  isGcsMetadataDetailSet: boolean = false
  hasDataMigrationStarted: boolean = false
  hasSchemaMigrationStarted: boolean = false
  hasForeignKeyUpdateStarted: boolean = false
  selectedMigrationMode: string = MigrationModes.schemaAndData
  connectionType: string = InputType.DirectConnect
  selectedMigrationType: string = MigrationTypes.bulkMigration
  isMigrationInProgress: boolean = false
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
    MigrationJobId: '',
    DatabaseName: '',
    DatabaseUrl: '',
    BucketName: '',
    BucketUrl: '',
    MonitoringDashboardName:'',
    MonitoringDashboardUrl:'',
    AggMonitoringDashboardName:'',
    AggMonitoringDashboardUrl:'',
    ShardToShardResourcesMap: new Map<string, ResourceDetails[]>(),
  }
  generatedResourcesColumns = ['shardId', 'resourceType', 'resourceName', 'resourceUrl']
  
  @ViewChild(MatPaginator)
  paginator!: MatPaginator
  
  displayedResources: ResourceDetails[] = []
  displayedResourcesDataSource: MatTableDataSource<ResourceDetails> = new MatTableDataSource<ResourceDetails>(this.displayedResources)
  configuredMigrationProfile!: IMigrationProfile
  region: string = ''
  instance: string = ''
  dialect: string = ''
  isSharded: boolean = false
  numberOfShards: string = '0'
  numberOfInstances: string = '0'
  nodeCount: number = 0
  processingUnits: number = 0

  targetDetails: ITargetDetails = {
    TargetDB: localStorage.getItem(TargetDetails.TargetDB) as string,
    GcsMetadataPath: {
      GcsBucketName: localStorage.getItem(TargetDetails.GcsMetadataName) as string,
      GcsBucketRootPath: localStorage.getItem(TargetDetails.GcsMetadataRootPath) || '',
    },
    DefaultTimezone: localStorage.getItem(TargetDetails.DefaultTimezone) as string,
  }

  gcsConfig: IGcsConfig = {
    ttlInDays: localStorage.getItem(Gcs.TtlInDays) as string,
    ttlInDaysSet: (localStorage.getItem(Gcs.TtlInDaysSet) as string === 'true')
  }
  spannerConfig: ISpannerConfig = {
    GCPProjectID: '',
    SpannerInstanceID: '',
    IsMetadataDbCreated: false,
    IsConfigValid: false
  }
  skipForeignKeyResponseList = [
    { value: false, displayName: 'No' },
    { value: true, displayName: 'Yes' },
  ]

  migrationModesHelpText = new Map<string, string>([
    ["Schema", "Migrates only the schema of the source database to the configured Spanner instance."],
    ["Data", "Migrates the data from the source database to the configured Spanner database. The configured database should already contain the schema."],
    ["Schema And Data", "Migrates both the schema and the data from the source database to Spanner."]
  ]);

  migrationTypesHelpText = new Map<string, string>([
    ["bulk", "Use the POC migration option when you want to migrate a sample of your data (<100GB) to do a Proof of Concept. It uses this machine's resources to copy data from the source database to Spanner"],
  ]);

  resourceTypeToDisplayTest = new Map<string, string>([
    ["gcs", "Cloud Storage Bucket"],
    ["monitoring", "Monitoring Dashboard"],
  ]);

  refreshMigrationMode() {
    this.selectedMigrationType = MigrationTypes.bulkMigration
    this.migrationTypes = [
      {
        name: 'POC Migration',
        value: MigrationTypes.bulkMigration,
      },
    ]
  }

  refreshPrerequisites() {
    this.isSourceConnectionProfileSet = false
    this.isTargetConnectionProfileSet = false
    this.isTargetDetailSet = false
    this.isGcsConfigurationSet = false
    this.isGcsMetadataDetailSet = false
    this.refreshMigrationMode()
  }

  ngOnInit(): void {
    this.initializeFromLocalStorage()
    this.data.config.subscribe((res: ISpannerConfig) => {
      this.spannerConfig = res
    })
    this.convObj = this.data.conv.subscribe((data: IConv) => {
      this.conv = data
    })
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
        this.isSharded = res.IsSharded
        this.processingUnits = res.ProcessingUnits
        this.nodeCount = res.NodeCount
        if (
          res.DatabaseType == SourceDbNames.MySQL.toLowerCase() ||
          res.DatabaseType == SourceDbNames.Oracle.toLowerCase() ||
          res.DatabaseType == SourceDbNames.Postgres.toLowerCase()
        ) {
        }
        this.selectedMigrationType = MigrationTypes.bulkMigration
        this.migrationTypes = [
          {
            name: 'POC Migration',
            value: MigrationTypes.bulkMigration,
          },
        ]
        this.sourceDatabaseName = res.SourceDatabaseName
        this.migrationModes = [
          MigrationModes.schemaOnly,
          MigrationModes.dataOnly,
          MigrationModes.schemaAndData,
        ]
        if (this.sourceDatabaseType === SourceDbNames.Cassandra) {
          this.migrationModes = [MigrationModes.schemaOnly]
          this.selectedMigrationMode = MigrationModes.schemaOnly
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
    if (localStorage.getItem(MigrationDetails.isForeignKeySkipped) != null) {
      this.isForeignKeySkipped = localStorage.getItem(MigrationDetails.isForeignKeySkipped) === 'true'
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
    if (localStorage.getItem(Gcs.IsGcsConfigSet) != null) {
      this.isGcsConfigurationSet = (localStorage.getItem(Gcs.IsGcsConfigSet) as string === 'true')
    }
    if (localStorage.getItem(MigrationDetails.IsGcsMetadataPathSet) != null) {
      this.isGcsMetadataDetailSet = (localStorage.getItem(MigrationDetails.IsGcsMetadataPathSet) as string) === 'true'
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
    if (localStorage.getItem(MigrationDetails.NumberOfShards) != null) {
      this.numberOfShards = localStorage.getItem(MigrationDetails.NumberOfShards) as string
    }
    if (localStorage.getItem(MigrationDetails.NumberOfInstances) != null) {
      this.numberOfInstances = localStorage.getItem(MigrationDetails.NumberOfInstances) as string
    }
  }

  clearLocalStorage() {
    localStorage.removeItem(MigrationDetails.MigrationMode)
    localStorage.removeItem(MigrationDetails.MigrationType)
    localStorage.removeItem(MigrationDetails.IsTargetDetailSet)
    localStorage.removeItem(MigrationDetails.IsGcsMetadataPathSet)
    localStorage.removeItem(MigrationDetails.isForeignKeySkipped)
    localStorage.removeItem(MigrationDetails.IsSourceConnectionProfileSet)
    localStorage.removeItem(MigrationDetails.IsTargetConnectionProfileSet)
    localStorage.removeItem(MigrationDetails.IsSourceDetailsSet)
    localStorage.removeItem(Gcs.IsGcsConfigSet)
    localStorage.removeItem(Gcs.TtlInDays)
    localStorage.removeItem(Gcs.TtlInDaysSet)
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
    localStorage.removeItem(MigrationDetails.NumberOfShards)
    localStorage.removeItem(MigrationDetails.NumberOfInstances)
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

  openShardedBulkSourceDetailsForm() {
    let payload: IShardSessionDetails = {
      sourceDatabaseEngine: this.sourceDatabaseType,
      isRestoredSession: this.connectionType
    }
    let dialogRef = this.dialog.open(ShardedBulkSourceDetailsFormComponent, {
      width: '30vw',
      minWidth: '400px',
      maxWidth: '550px',
      data: payload
    })
    dialogRef.afterClosed().subscribe(() => {
      this.isSourceDetailsSet = localStorage.getItem(MigrationDetails.IsSourceDetailsSet) as string === 'true'
      if (localStorage.getItem(MigrationDetails.NumberOfShards) != null) {
        this.numberOfShards = localStorage.getItem(MigrationDetails.NumberOfShards) as string
      }
      if (localStorage.getItem(MigrationDetails.NumberOfInstances) != null) {
        this.numberOfInstances = localStorage.getItem(MigrationDetails.NumberOfInstances) as string
      }
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
        GcsMetadataPath: {
          GcsBucketName: localStorage.getItem(TargetDetails.GcsMetadataName) as string,
          GcsBucketRootPath: localStorage.getItem(TargetDetails.GcsMetadataRootPath) as string,
        },
        DefaultTimezone: localStorage.getItem(TargetDetails.DefaultTimezone) as string,
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

  openGcsMetadataDetailsForm() {
    let dialogRef = this.dialog.open(GcsMetadataDetailsFormComponent, dialogDefault)
    dialogRef.afterClosed().subscribe(() => {
      this.isGcsMetadataDetailSet = localStorage.getItem(MigrationDetails.IsGcsMetadataPathSet) as string === 'true'
      this.targetDetails = {
        TargetDB: localStorage.getItem(TargetDetails.TargetDB) as string,
        GcsMetadataPath: {
          GcsBucketName: localStorage.getItem(TargetDetails.GcsMetadataName) as string,
          GcsBucketRootPath: localStorage.getItem(TargetDetails.GcsMetadataRootPath) as string,
        },
        DefaultTimezone: localStorage.getItem(TargetDetails.DefaultTimezone) as string,
      }
    })
  }

  migrate() {
    this.resetValues()
    let payload: IMigrationDetails = {
      TargetDetails: this.targetDetails,
      GcsConfig: this.gcsConfig,
      IsSharded: this.isSharded,
      MigrationType: this.selectedMigrationType,
      MigrationMode: this.selectedMigrationMode,
      skipForeignKeys: this.isForeignKeySkipped
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
              } else {
                this.markSchemaMigrationComplete()
                this.hasDataMigrationStarted = true
                localStorage.setItem(
                  MigrationDetails.HasDataMigrationStarted,
                  this.hasDataMigrationStarted.toString()
                )
              }
            } else if (res.ProgressStatus == ProgressStatus.DataMigrationComplete) {
              this.hasDataMigrationStarted = true
              localStorage.setItem(
                MigrationDetails.HasDataMigrationStarted,
                this.hasDataMigrationStarted.toString()
              )
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

  downloadConfiguration() {
    this.fetch.getSourceProfile().subscribe({
      next: (res: IMigrationProfile) => {
        this.configuredMigrationProfile = res
        var a = document.createElement('a')
        // JS automatically converts the input (64bit INT) to '9223372036854776000' during conversion as this is the max value in JS.
        // However the max value received from server is '9223372036854775807'
        // Therefore an explicit replacement is necessary in the JSON content in the file.
        let resJson = JSON.stringify(this.configuredMigrationProfile, null, '\t').replace(/9223372036854776000/g, '9223372036854775807')
        a.href = 'data:text/json;charset=utf-8,' + encodeURIComponent(resJson)
        a.download = localStorage.getItem(TargetDetails.TargetDB) as string + "-" + this.configuredMigrationProfile.configType + `-shardConfig.cfg`
        a.click()
      },
      error: (err: any) => {
        this.snack.openSnackBar(err.error, 'Close')
      },
    })

  }

  fetchGeneratedResources() {
    this.fetch.getGeneratedResources().subscribe({
      next: (res: IGeneratedResources) => {
        this.isResourceGenerated = true
        this.resourcesGenerated = res
        //casting to map is required.
        this.resourcesGenerated.ShardToShardResourcesMap = new Map<string, ResourceDetails[]>(Object.entries(this.resourcesGenerated.ShardToShardResourcesMap))
        this.prepareGeneratedResourcesTableData(this.resourcesGenerated)
      },
      error: (err: any) => {
        this.snack.openSnackBar(err.error, 'Close')
      },
    })
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
      MigrationJobId: '',
      DatabaseName: '',
      DatabaseUrl: '',
      BucketName: '',
      BucketUrl: '',
      MonitoringDashboardName:'',
      MonitoringDashboardUrl:'',
      AggMonitoringDashboardName:'',
      AggMonitoringDashboardUrl:'',
      ShardToShardResourcesMap: new Map<string, ResourceDetails[]>(),
    }
    this.initializeLocalStorage()
  }
  initializeLocalStorage() {
    localStorage.setItem(MigrationDetails.MigrationMode, this.selectedMigrationMode)
    localStorage.setItem(MigrationDetails.MigrationType, this.selectedMigrationType)
    localStorage.setItem(MigrationDetails.isForeignKeySkipped, this.isForeignKeySkipped.toString())
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
    localStorage.setItem(MigrationDetails.IsGcsMetadataPathSet, this.isGcsMetadataDetailSet.toString())
  }

  openSaveSessionSidenav() {
    this.sidenav.openSidenav()
    this.sidenav.setSidenavComponent('saveSession')
    this.sidenav.setSidenavDatabaseName(this.conv.DatabaseName)
  }
  downloadSession() {
    downloadSession(this.conv)
  }

  downloadOverrides() {
    downloadOverrides(this.conv)
  }

  prepareGeneratedResourcesTableData(resourcesGenerated: IGeneratedResources) {
    for (let [shardId, resourceList] of resourcesGenerated.ShardToShardResourcesMap) {
     for (let resource of resourceList) {
       resource.DataShardId = shardId
     }
     this.displayedResources.push(...resourceList) 
    }
    this.displayedResourcesDataSource = new MatTableDataSource(this.displayedResources)
    this.displayedResourcesDataSource.paginator = this.paginator
  }

  ngOnDestroy() { }
}
