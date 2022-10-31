import { Component, OnInit } from '@angular/core'
import IUpdateTable, { IReviewInterleaveTableChanges } from 'src/app/model/update-table'
import { DataService } from 'src/app/services/data/data.service'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service'
import { TableUpdatePubSubService } from 'src/app/services/table-update-pub-sub/table-update-pub-sub.service'

@Component({
  selector: 'app-sidenav-review-changes',
  templateUrl: './sidenav-review-changes.component.html',
  styleUrls: ['./sidenav-review-changes.component.scss'],
})
export class SidenavReviewChangesComponent implements OnInit {
  ddl: string = ''
  showDdl: boolean = true
  tableUpdateData: { tableName: string; updateDetail: IUpdateTable } = {
    tableName: '',
    updateDetail: { UpdateCols: {} },
  }
  tableChanges: IReviewInterleaveTableChanges[] = []
  tableNames: string[] = []
  tableList: string = ''

  constructor(
    private sidenav: SidenavService,
    private tableUpdatePubSub: TableUpdatePubSubService,
    private data: DataService,
    private snackbar: SnackbarService
  ) {}

  ngOnInit(): void {
    this.tableUpdatePubSub.reviewTableChanges.subscribe((data) => {
      if (data.Changes && data.Changes.length > 0) {
        this.showDdl = false
        this.tableChanges = data.Changes
        const updatedTableNames: string[] = []
        this.tableList = ''
        this.tableChanges.forEach((data, index) => {
          updatedTableNames.push(data.Table)
          if (index == 0) {
            this.tableList += data.Table
          } else {
            this.tableList += ', ' + data.Table
          }
        })
        this.tableList += '.'
        this.tableNames = updatedTableNames
      } else {
        this.showDdl = true
        this.ddl = data.DDL
      }
    })
    this.tableUpdatePubSub.tableUpdateDetail.subscribe((data) => {
      this.tableUpdateData = data
    })
  }

  updateTable() {
    this.data
      .updateTable(this.tableUpdateData.tableName, this.tableUpdateData.updateDetail)
      .subscribe({
        next: (res: string) => {
          if (res == '') {
            this.snackbar.openSnackBar(
              `Schema changes to table ${this.tableUpdateData.tableName} saved successfully`,
              'Close',
              5
            )
            this.closeSidenav()
          } else {
            this.snackbar.openSnackBar(res, 'Close', 5)
          }
        },
      })
  }

  closeSidenav(): void {
    this.ddl = ''
    this.sidenav.closeSidenav()
  }
}
