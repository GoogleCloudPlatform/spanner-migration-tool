import { Component, OnInit } from '@angular/core'
import ITableColumnChanges from 'src/app/model/table-column-changes'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'

@Component({
  selector: 'app-sidenav-review-changes',
  templateUrl: './sidenav-review-changes.component.html',
  styleUrls: ['./sidenav-review-changes.component.scss'],
})
export class SidenavReviewChangesComponent implements OnInit {
  ddl: string = ''
  showDdl: boolean = false
  tableChanges: ITableColumnChanges[][] = [
    [
      {
        ColumnName: 'product',
        Type: 'string',
        Pk: false,
        UpdatedColumnName: 'product_v2',
        UpdatedType: 'byte',
        UpdatedPk: false,
      },
      {
        ColumnName: 'product_id',
        Type: 'string',
        Pk: true,
        UpdatedColumnName: 'product_id_v2',
        UpdatedType: 'string',
        UpdatedPk: true,
      },
    ],
    [
      {
        ColumnName: 'product',
        Type: 'string',
        Pk: false,
        UpdatedColumnName: 'product_v2',
        UpdatedType: 'byte',
        UpdatedPk: false,
      },
      {
        ColumnName: 'product_id',
        Type: 'string',
        Pk: true,
        UpdatedColumnName: 'product_id_v2',
        UpdatedType: 'string',
        UpdatedPk: true,
      },
    ],
    [
      {
        ColumnName: 'product',
        Type: 'string',
        Pk: false,
        UpdatedColumnName: 'product_v2',
        UpdatedType: 'byte',
        UpdatedPk: false,
      },
      {
        ColumnName: 'product_id',
        Type: 'string',
        Pk: true,
        UpdatedColumnName: 'product_id_v2',
        UpdatedType: 'string',
        UpdatedPk: true,
      },
    ],
    [
      {
        ColumnName: 'product',
        Type: 'string',
        Pk: false,
        UpdatedColumnName: 'product_v2',
        UpdatedType: 'byte',
        UpdatedPk: false,
      },
      {
        ColumnName: 'product_id',
        Type: 'string',
        Pk: true,
        UpdatedColumnName: 'product_id_v2',
        UpdatedType: 'string',
        UpdatedPk: true,
      },
    ],
  ]

  constructor(private sidenav: SidenavService) {}

  ngOnInit(): void {
    this.ddl =
      '--\n-- Spanner schema for source table cart\n--\nCREATE TABLE cart (\n\tuser_id STRING(20) NOT NULL,    -- From: user_id varchar(20)\n\tproduct_id STRING(20) NOT NULL, -- From: product_id varchar(20)\n\tquantity INT64,                 -- From: quantity bigint(20)\n) PRIMARY KEY (user_id, product_id)'
    this.showDdl = Math.floor(Math.random() * 100) % 2 === 0
  }

  closeSidenav(): void {
    this.sidenav.closeSidenav()
  }
}
