import { Component, Input, OnInit, SimpleChanges } from '@angular/core'
import { IReviewInterleaveTableChanges, ITableColumnChanges } from 'src/app/model/update-table'

@Component({
  selector: 'app-table-column-changes-preview',
  templateUrl: './table-column-changes-preview.component.html',
  styleUrls: ['./table-column-changes-preview.component.scss'],
})
export class TableColumnChangesPreviewComponent implements OnInit {
  @Input() tableChange: IReviewInterleaveTableChanges = { InterleaveColumnChanges: [], Table: '' }
  dataSource: ITableColumnChanges[] = []
  displayedColumns: string[] = ['ColumnName', 'Type', 'UpdateColumnName', 'UpdateType']

  constructor() {}

  ngOnInit(): void {}
  ngOnChanges(changes: SimpleChanges): void {
    this.tableChange = changes['tableChange']?.currentValue || this.tableChange
    this.dataSource = this.tableChange.InterleaveColumnChanges
  }
}
