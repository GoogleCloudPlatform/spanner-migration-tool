import { Component, OnDestroy, OnInit } from '@angular/core'
import { DataService } from 'src/app/services/data/data.service'
import { ConversionService } from '../../services/conversion/conversion.service'
import IConv from '../../model/Conv'
import { Subscription } from 'rxjs/internal/Subscription'
import { MatDialog } from '@angular/material/dialog'
import { SaveSessionFormComponent } from '../save-session-form/save-session-form.component'
import IColumnTabData from '../../model/ColumnTabData'

@Component({
  selector: 'app-workspace',
  templateUrl: './workspace.component.html',
  styleUrls: ['./workspace.component.scss'],
})
export class WorkspaceComponent implements OnInit, OnDestroy {
  conv!: IConv
  currentTable: string
  rowData: IColumnTabData[] = []
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
  isOfflineStatus: boolean = false

  constructor(
    private data: DataService,
    private conversion: ConversionService,
    private dialog: MatDialog
  ) {
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
      console.log('got the data.....', data)
      this.conv = data
      this.rowData = this.conversion.getColMap(this.currentTable, data)
      this.converObj = this.data.conversionRate.subscribe((rates: any) => {
        this.tableNames = Object.keys(this.conv.SpSchema)
        this.conversionRates = rates
      })
    })

    this.data.isOffline.subscribe({
      next: (res: boolean) => {
        this.isOfflineStatus = res
      },
    })
  }

  ngOnDestroy(): void {
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

  openSaveSessionModal() {
    this.dialog.open(SaveSessionFormComponent, { minWidth: '500px' })
  }
  downloadSession() {
    var a = document.createElement('a')
    let resJson = JSON.stringify(this.conv).replace(/9223372036854776000/g, '9223372036854775807')
    a.href = 'data:text/json;charset=utf-8,' + encodeURIComponent(resJson)
    a.download = 'session.json'
    a.click()
  }
}
