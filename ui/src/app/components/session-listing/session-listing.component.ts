import { Component, OnInit } from '@angular/core';

@Component({
  selector: 'app-session-listing',
  templateUrl: './session-listing.component.html',
  styleUrls: ['./session-listing.component.scss']
})
export class SessionListingComponent implements OnInit {

  // Temporary data - to be removed.
  displayedColumns = ['sessionName', 'lastUpdateDate', 'status', 'action'];
  dataSource = ELEMENT_DATA;
  
  constructor() { }

  ngOnInit(): void {
  }

}

export interface Session {
  Id: string
  SessionName: string;
  LastUpdateDate: string;
  Status: string;
}

const ELEMENT_DATA: Session[] = [
  {Id:'', SessionName: 'PG-Northwind-001', LastUpdateDate: '04/03/2022', Status: 'In progress'},
  {Id:'', SessionName: 'MYSQL-Northwind-001', LastUpdateDate: '04/02/2022', Status: 'Complete'},
  {Id:'', SessionName: 'PG-Northwind-001', LastUpdateDate: '22/02/2022', Status: 'In progress'},
  {Id:'', SessionName: 'MSSQL-Northwind-001', LastUpdateDate: '14/03/2022', Status: 'In progress'},
  {Id:'', SessionName: 'MYSQLDUMP-Northwind-001', LastUpdateDate: '21/03/2022', Status: 'Complete'},
  {Id:'', SessionName: 'ORCLE-Northwind-001', LastUpdateDate: '05/03/2022', Status: 'In progress'},
  {Id:'', SessionName: 'PG-Northwind-001', LastUpdateDate: '04/03/2022', Status: 'In progress'},
  {Id:'', SessionName: 'PGDUMP-Northwind-001', LastUpdateDate: '01/01/2022', Status: 'Aborted'},
  {Id:'', SessionName: 'MYSQLDUMP-Northwind-001', LastUpdateDate: '01/002/2022', Status: 'In progress'},
  {Id:'', SessionName: 'ORCL-Northwind-001', LastUpdateDate: '08/03/2022', Status: 'In progress'},
  {Id:'', SessionName: 'MSSQL-Northwind-001', LastUpdateDate: '18/02/2022', Status: 'In progress'},
];