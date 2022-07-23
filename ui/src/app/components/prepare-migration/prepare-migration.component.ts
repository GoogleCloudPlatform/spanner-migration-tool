import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-prepare-migration',
  templateUrl: './prepare-migration.component.html',
  styleUrls: ['./prepare-migration.component.scss']
})
export class PrepareMigrationComponent implements OnInit {

  constructor() { }

  ngOnInit(): void {
  }

}
import { Component, OnInit } from '@angular/core'
import { MatDialog } from '@angular/material/dialog'
import { TargetDetailsFormComponent } from '../target-details-form/target-details-form.component'
import { TargetDetailsService } from 'src/app/services/target-details/target-details.service'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service'
import ITargetDetails from 'src/app/model/target-details'
import { ISourceDestinationDetails } from 'src/app/model/conv'
@Component({
  selector: 'app-prepare-migration',
  templateUrl: './prepare-migration.component.html',
  styleUrls: ['./prepare-migration.component.scss'],
})
export class PrepareMigrationComponent implements OnInit {
  displayedColumns = ['Title', 'Source', 'Destination']
  dataSource : any =[]
  constructor(
    private dialog: MatDialog,
    private fetch: FetchService,
    private snack: SnackbarService,
    private targetDetailService: TargetDetailsService
  ) {}

  isTargetDetailSet: boolean = false;
  isStreamingCfgSet: boolean = false;
  targetDetails: ITargetDetails = this.targetDetailService.getTargetDetails()

  ngOnInit(): void {
      this.fetch.getSourceDestinationSummary().subscribe({
        next: (res: ISourceDestinationDetails) => {
          this.dataSource = [
            {title: 'Database driver', source:res.DatabaseType, target:'Spanner'},
            {title: 'Number of tables', source:res.SourceTableCount, target: res.SpannerTableCount},
            {title: 'Number of indexes', source:res.SourceIndexCount, target: res.SpannerIndexCount},
          ];
          console.log(this.dataSource)
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
    })
    dialogRef.afterClosed().subscribe(() => {
      if (this.targetDetails.TargetDB != '') {
        this.isTargetDetailSet = true;
      }
      if (this.targetDetails.StreamingConfig != '') {
        this.isStreamingCfgSet = true;
      }
    });
    console.log(this.targetDetailService.getTargetDetails())
  }

  migrate() {
    this.fetch.migrate(this.targetDetailService.getTargetDetails()).subscribe({
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