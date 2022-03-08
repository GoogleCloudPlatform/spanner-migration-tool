import { Component, Input, OnInit } from '@angular/core'
import { DataService } from 'src/app/services/data/data.service'
interface IColMap {
  srcColName: string
  srcDataType: string
  spColName: string
  spDataType: string
}
@Component({
  selector: 'app-workspace',
  templateUrl: './workspace.component.html',
  styleUrls: ['./workspace.component.scss'],
})
export class WorkspaceComponent implements OnInit {
  conv!: any
  currentTable = 'AllTypes'
  rowData!: IColMap[]
  typeMap: any = {}
  constructor(private data: DataService) {}

  ngOnInit(): void {
    this.data.conv.subscribe((data) => {
      this.conv = data
      this.rowData = data['mapping'][this.currentTable]
    })
  }

  changeCurrentTable(table: string) {
    this.currentTable = table
    this.rowData = this.conv['mapping'][this.currentTable]
  }
}
