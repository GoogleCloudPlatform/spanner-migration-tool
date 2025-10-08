import { Component, OnInit } from '@angular/core'
import IUpdateTable from 'src/app/model/update-table'
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
  tableUpdateData: { tableName: string; tableId: string; updateDetail: IUpdateTable } = {
    tableName: '',
    tableId: '',
    updateDetail: { UpdateCols: {} },
  }

  constructor(
    private sidenav: SidenavService,
    private tableUpdatePubSub: TableUpdatePubSubService,
    private data: DataService,
    private snackbar: SnackbarService
  ) {}

  ngOnInit(): void {
    this.tableUpdatePubSub.reviewTableChanges.subscribe((data) => {
      this.showDdl = true
      this.ddl = data.DDL
    })
    this.tableUpdatePubSub.tableUpdateDetail.subscribe((data) => {
      this.tableUpdateData = data
    })
  }

  updateTable() {
    this.data
      .updateTable(this.tableUpdateData.tableId, this.tableUpdateData.updateDetail)
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
