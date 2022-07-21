import { Component, OnInit } from '@angular/core'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'

@Component({
  selector: 'app-sidenav-review-changes',
  templateUrl: './sidenav-review-changes.component.html',
  styleUrls: ['./sidenav-review-changes.component.scss'],
})
export class SidenavReviewChangesComponent implements OnInit {
  ddl: string = ''

  constructor(private sidenav: SidenavService) {}

  ngOnInit(): void {
    this.ddl =
      '--\n-- Spanner schema for source table cart\n--\nCREATE TABLE cart (\n\tuser_id STRING(20) NOT NULL,    -- From: user_id varchar(20)\n\tproduct_id STRING(20) NOT NULL, -- From: product_id varchar(20)\n\tquantity INT64,                 -- From: quantity bigint(20)\n) PRIMARY KEY (user_id, product_id)'
  }

  closeSidenav(): void {
    this.sidenav.closeSidenav()
  }
}
