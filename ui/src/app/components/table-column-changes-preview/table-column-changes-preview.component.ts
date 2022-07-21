import { Component, Input, OnInit } from '@angular/core'
import ITableColumnChanges from 'src/app/model/table-column-changes'

@Component({
  selector: 'app-table-column-changes-preview',
  templateUrl: './table-column-changes-preview.component.html',
  styleUrls: ['./table-column-changes-preview.component.scss'],
})
export class TableColumnChangesPreviewComponent implements OnInit {
  @Input() dataSource: ITableColumnChanges[] = []
  displayedColumns: string[] = [
    'ColumnName',
    'Type',
    'Pk',
    'UpdatedColumnName',
    'UpdatedType',
    'UpdatedPk',
  ]

  constructor() {}

  ngOnInit(): void {}
}
