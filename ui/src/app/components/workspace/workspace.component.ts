import { Component, Input, OnInit } from '@angular/core'
import { DataService } from 'src/app/services/data/data.service'
import { ConversionService } from '../../services/conversion/conversion.service'
import IConv from '../../model/Conv'
import { concatMap } from 'rxjs/operators'
import ISchemaObjectNode from '../../model/SchemaObjectNode'
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
  conv!: IConv
  currentTable = 'AllTypes'
  rowData: IColMap[] = []
  typeMap: Record<string, Record<string, string>> | boolean = false
  tableNames: string[] = []
  conversionRates: Record<string, string> = {}

  constructor(private data: DataService, private conversion: ConversionService) {}

  ngOnInit(): void {
    this.data.getSchemaConversionData()
    this.data.typeMap.subscribe((types) => {
      this.typeMap = types
    })

    this.data.conv.subscribe((data: IConv) => {
      this.conv = data
      this.rowData = this.conversion.getColMap(this.currentTable, data)
    })

    this.data.conversionRate.subscribe((rates: any) => {
      this.tableNames = Object.keys(this.conv.SpSchema)
      this.conversionRates = rates
    })
  }

  changeCurrentTable(table: string) {
    this.currentTable = table
    this.rowData = this.conversion.getColMap(this.currentTable, this.conv)
  }
}
