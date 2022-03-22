import { Component, OnDestroy, OnInit } from '@angular/core'
import { DataService } from 'src/app/services/data/data.service'
import { ConversionService } from '../../services/conversion/conversion.service'
import IConv from '../../model/Conv'
import { Subscription } from 'rxjs/internal/Subscription'
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
export class WorkspaceComponent implements OnInit, OnDestroy {
  conv!: IConv
  currentTable: string
  rowData: IColMap[] = []
  typeMap: Record<string, Record<string, string>> | boolean = false
  tableNames: string[] = []
  conversionRates: Record<string, string> = {}
  typemapObj!: Subscription
  convObj!: Subscription
  converObj!: Subscription
  ddlsumconvObj!: Subscription
  ddlObj!: Subscription
  isLeftColumnCollapse: boolean = false
  isRightColumnCollapse: boolean = true
  ddlStmts: any

  constructor(private data: DataService, private conversion: ConversionService) {
    this.currentTable = ''
  }

  ngOnInit(): void {
    this.ddlsumconvObj = this.data.getRateTypemapAndSummary()

    this.typemapObj = this.data.typeMap.subscribe((types) => {
      this.typeMap = types
    })

    this.ddlObj = this.data.ddl.subscribe((res) => {
      this.ddlStmts = res
    })

    this.convObj = this.data.conv.subscribe((data: IConv) => {
      this.conv = data
      this.rowData = this.conversion.getColMap(this.currentTable, data)

      this.converObj = this.data.conversionRate.subscribe((rates: any) => {
        this.tableNames = Object.keys(this.conv.SpSchema)
        this.conversionRates = rates
      })
    })
  }

  ngOnDestroy(): void {
    console.log('workspace Destroy !! ')
    this.typemapObj.unsubscribe()
    this.convObj.unsubscribe()
    // this.converObj.unsubscribe()
    this.ddlObj.unsubscribe()
    this.ddlsumconvObj.unsubscribe()
  }

  changeCurrentTable(table: string) {
    this.currentTable = table
    this.rowData = this.conversion.getColMap(this.currentTable, this.conv)
  }

  leftColumnToggle() {
    this.isLeftColumnCollapse = !this.isLeftColumnCollapse
  }

  rightColumnToggle() {
    this.isRightColumnCollapse = !this.isRightColumnCollapse
  }
}
