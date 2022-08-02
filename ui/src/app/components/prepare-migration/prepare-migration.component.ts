import { Component, OnInit } from '@angular/core'
import { MatDialog } from '@angular/material/dialog'
import { TargetDetailsFormComponent } from '../target-details-form/target-details-form.component'
import { TargetDetailsService } from 'src/app/services/target-details/target-details.service'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service'
import ITargetDetails from 'src/app/model/target-details'
import { ISessionSummary } from 'src/app/model/conv'
import IMigrationDetails from 'src/app/model/migrate'
@Component({
  selector: 'app-prepare-migration',
  templateUrl: './prepare-migration.component.html',
  styleUrls: ['./prepare-migration.component.scss'],
})
export class PrepareMigrationComponent implements OnInit {
  displayedColumns = ['Title', 'Source', 'Destination']
  dataSource: any = []
  migrationModes: any = []
  constructor(
    private dialog: MatDialog,
    private fetch: FetchService,
    private snack: SnackbarService,
    private targetDetailService: TargetDetailsService
  ) {}

  isTargetDetailSet: boolean = false
  isStreamingCfgSet: boolean = false
  isSchemaMigration: boolean = true
  isBulkMigration: boolean = true
  selectedMigrationMode: string = 'Schema'
  selectedMigrationType: string = 'bulk'
  targetDetails: ITargetDetails = this.targetDetailService.getTargetDetails()

  ngOnInit(): void {
    this.fetch.getSourceDestinationSummary().subscribe({
      next: (res: ISessionSummary) => {
        this.dataSource = [
          { title: 'Database driver', source: res.DatabaseType, target: 'Spanner' },
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
        if (res.ConnectionType == 'dump') {
          this.migrationModes = ['Schema and Data']
        } else if (res.ConnectionType == 'session') {
          this.migrationModes = ['Schema']
        } else {
          this.migrationModes = ['Schema', 'Data', 'Schema-and-Data']
        }
        if (res.DatabaseType == 'mysql' || res.DatabaseType == 'oracle') {
          this.isBulkMigration = false
        }
        console.log(res)
      },
      error: (err: any) => {
        console.log(err.error)
        // this.snackbar.openSnackBar(err.error, 'Close')
      },
    })
  }

  openTargetDetailsForm() {
    let dialogRef = this.dialog.open(TargetDetailsFormComponent, {
      width: '30vw',
      minWidth: '400px',
      maxWidth: '500px',
      data:this.selectedMigrationType=='lowdt',
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
    let payload: IMigrationDetails = {
      TargetDetails: this.targetDetailService.getTargetDetails(),
      MigrationType: this.selectedMigrationType,
      MigrationMode: this.selectedMigrationMode
    }
    console.log(payload)
    this.fetch.migrate(payload).subscribe({
      next: () => {
        if (this.isStreamingCfgSet) {
          this.snack.openSnackBar('Migration started successfully', 'Close', 5)
        } else {
          this.snack.openSnackBar('Migration completed successfully', 'Close', 5)
        }
      },
      error: (err: any) => {
        this.snack.openSnackBar(err.error, 'Close')
      },
    })
  }
}
