import { Component, OnDestroy, OnInit } from '@angular/core'
import { DataService } from 'src/app/services/data/data.service'
import { ConversionService } from '../../services/conversion/conversion.service'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'
import IConv, { ITableIdAndName } from '../../model/conv'
import { Subscription } from 'rxjs/internal/Subscription'
import { MatDialog } from '@angular/material/dialog'
import IFkTabData from 'src/app/model/fk-tab-data'
import IColumnTabData, { IIndexData } from '../../model/edit-table'
import ISchemaObjectNode, { FlatNode } from 'src/app/model/schema-object-node'
import { InputType, ObjectExplorerNodeType, StorageKeys } from 'src/app/app.constants'
import { IUpdateTableArgument } from 'src/app/model/update-table'
import ConversionRate from 'src/app/model/conversion-rate'
import { Router } from '@angular/router'
import { extractSourceDbName } from 'src/app/utils/utils'
import { ClickEventService } from 'src/app/services/click-event/click-event.service'
import IViewAssesmentData from 'src/app/model/view-assesment'
import IDbConfig from 'src/app/model/db-config'
import { InfodialogComponent } from '../infodialog/infodialog.component'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import IStructuredReport from '../../model/structured-report'

@Component({
  selector: 'app-workspace',
  templateUrl: './workspace.component.html',
  styleUrls: ['./workspace.component.scss'],
})
export class WorkspaceComponent implements OnInit, OnDestroy {
  conv!: IConv
  fkData: IFkTabData[] = []
  currentObject: FlatNode | null
  tableData: IColumnTabData[] = []
  indexData: IIndexData[] = []
  typeMap: Record<string, Record<string, string>> | boolean = false
  conversionRates: Record<string, string> = {}
  typemapObj!: Subscription
  convObj!: Subscription
  converObj!: Subscription
  ddlsumconvObj!: Subscription
  ddlObj!: Subscription
  isLeftColumnCollapse: boolean = false
  isRightColumnCollapse: boolean = true
  isMiddleColumnCollapse: boolean = true
  ddlStmts: any
  isOfflineStatus: boolean = false
  spannerTree: ISchemaObjectNode[] = []
  srcTree: ISchemaObjectNode[] = []
  issuesAndSuggestionsLabel: string = 'ISSUES AND SUGGESTIONS'
  rulesLabel: string = 'RULES (0)'
  objectExplorerInitiallyRender: boolean = false
  srcDbName: string = localStorage.getItem(StorageKeys.SourceDbName) as string
  conversionRateCount: ConversionRate = { good: 0, ok: 0, bad: 0 }
  conversionRatePercentages: ConversionRate = { good: 0, ok: 0, bad: 0 }
  currentDatabase: string = 'spanner'
  dialect: string = ''
  structuredReport!: IStructuredReport
  constructor(
    private data: DataService,
    private conversion: ConversionService,
    private dialog: MatDialog,
    private sidenav: SidenavService,
    private router: Router,
    private clickEvent: ClickEventService,
    private fetch: FetchService,
  ) {
    this.currentObject = null
  }

  ngOnInit(): void {
    this.conversion.getStandardTypeToPGSQLTypemap()
    this.conversion.getPGSQLToStandardTypeTypemap()
    this.ddlsumconvObj = this.data.getRateTypemapAndSummary()
    this.typemapObj = this.data.typeMap.subscribe((types) => {
      this.typeMap = types
    })

    this.ddlObj = this.data.ddl.subscribe((res) => {
      this.ddlStmts = res
    })

    this.sidenav.setMiddleColumnComponent.subscribe((flag: boolean) => {
      this.isMiddleColumnCollapse = !flag
    })

    this.convObj = this.data.conv.subscribe((data: IConv) => {
      if (Object.keys(data.SrcSchema).length <= 0) {
        this.router.navigate(['/'])
      }
      const indexAddedOrRemoved = this.isIndexAddedOrRemoved(data)
      if (
        data &&
        this.conv &&
        Object.keys(data?.SpSchema).length != Object.keys(this.conv?.SpSchema).length
      ) {
        this.conv = data
        this.reRenderObjectExplorerSpanner()
        this.reRenderObjectExplorerSrc()
      }
      this.conv = data
      if (this.conv.DatabaseType) {
        this.srcDbName = extractSourceDbName(this.conv.DatabaseType)
      }

      if (indexAddedOrRemoved && this.conversionRates) this.reRenderObjectExplorerSpanner()
      if (!this.objectExplorerInitiallyRender && this.conversionRates) {
        this.reRenderObjectExplorerSpanner()
        this.reRenderObjectExplorerSrc()
        this.objectExplorerInitiallyRender = true
      }
      if (this.currentObject && this.currentObject.type === ObjectExplorerNodeType.Table) {
        this.fkData = this.currentObject
          ? this.conversion.getFkMapping(this.currentObject.id, data)
          : []

        this.tableData = this.currentObject
          ? this.conversion.getColumnMapping(this.currentObject.id, data)
          : []
      }
      if (
        this.currentObject &&
        this.currentObject?.type === ObjectExplorerNodeType.Index &&
        !indexAddedOrRemoved
      ) {
        this.indexData = this.conversion.getIndexMapping(
          this.currentObject.parentId,
          this.conv,
          this.currentObject.id
        )
      }
      this.dialect = (this.conv.SpDialect === "postgresql") ? "PostgreSQL" : "Google Standard SQL"
    })

    this.converObj = this.data.conversionRate.subscribe((rates: any) => {
      this.conversionRates = rates
      this.updateConversionRatePercentages()

      if (this.conv) {
        this.reRenderObjectExplorerSpanner()
        this.reRenderObjectExplorerSrc()
        this.objectExplorerInitiallyRender = true
      } else {
        this.objectExplorerInitiallyRender = false
      }
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
    this.ddlObj.unsubscribe()
    this.ddlsumconvObj.unsubscribe()
  }

  updateConversionRatePercentages() {
    let tableCount: number = Object.keys(this.conversionRates).length
    this.conversionRateCount = { good: 0, ok: 0, bad: 0 }
    this.conversionRatePercentages = { good: 0, ok: 0, bad: 0 }

    for (const rate in this.conversionRates) {
      if (this.conversionRates[rate] === 'NONE' || this.conversionRates[rate] === 'EXCELLENT') {
        this.conversionRateCount.good += 1
      } else if (this.conversionRates[rate] === 'GOOD' || this.conversionRates[rate] === 'OK') {
        this.conversionRateCount.ok += 1
      } else {
        this.conversionRateCount.bad += 1
      }
    }
    if (tableCount > 0) {
      for (let key in this.conversionRatePercentages) {
        this.conversionRatePercentages[key as keyof ConversionRate] = Number(
          ((this.conversionRateCount[key as keyof ConversionRate] / tableCount) * 100).toFixed(2)
        )
      }
    }
  }

  reRenderObjectExplorerSpanner() {
    this.spannerTree = this.conversion.createTreeNode(this.conv, this.conversionRates)
  }
  reRenderObjectExplorerSrc() {
    this.srcTree = this.conversion.createTreeNodeForSource(this.conv, this.conversionRates)
  }

  reRenderSidebar() {
    this.reRenderObjectExplorerSpanner()
  }

  changeCurrentObject(object: FlatNode) {
    if (object?.type === ObjectExplorerNodeType.Table) {
      this.currentObject = object
      this.tableData = this.currentObject
        ? this.conversion.getColumnMapping(this.currentObject.id, this.conv)
        : []

      this.fkData = []
      this.fkData = this.currentObject
        ? this.conversion.getFkMapping(this.currentObject.id, this.conv)
        : []
    } else if (object?.type === ObjectExplorerNodeType.Index) {
      this.currentObject = object
      this.indexData = this.conversion.getIndexMapping(object.parentId, this.conv, object.id)
    } else {
      this.currentObject = null
    }
  }

  changeCurrentDatabase(database: string) {
    this.currentDatabase = database
  }

  updateIssuesLabel(count: number) {
    setTimeout(() => {
      this.issuesAndSuggestionsLabel = `ISSUES AND SUGGESTIONS (${count})`
    })
  }
  updateRulesLabel(count: number) {
    setTimeout(() => {
      this.rulesLabel = `RULES (${count})`
    })
  }
  leftColumnToggle() {
    this.isLeftColumnCollapse = !this.isLeftColumnCollapse
  }

  middleColumnToggle() {
    this.isMiddleColumnCollapse = !this.isMiddleColumnCollapse
  }

  rightColumnToggle() {
    this.isRightColumnCollapse = !this.isRightColumnCollapse
  }

  openAssessment() {
    this.sidenav.openSidenav()
    this.sidenav.setSidenavComponent('assessment')
    let connectionDetail: string = ''
    let inputType = localStorage.getItem(StorageKeys.Type) as string
    if (inputType == InputType.DirectConnect) {
      let config: IDbConfig = JSON.parse(localStorage.getItem(StorageKeys.Config)!)
      connectionDetail = config?.hostName + ' : ' + config?.port
    } else {
      {
        connectionDetail = this.conv.DatabaseName
      }
    }
    let viewAssesmentData: IViewAssesmentData = {
      srcDbType: this.srcDbName,
      connectionDetail: connectionDetail,
      conversionRates: this.conversionRateCount,
    }
    this.clickEvent.setViewAssesmentData(viewAssesmentData)
  }
  openSaveSessionSidenav() {
    this.sidenav.openSidenav()
    this.sidenav.setSidenavComponent('saveSession')
    this.sidenav.setSidenavDatabaseName(this.conv.DatabaseName)
  }
  downloadSession() {
    var a = document.createElement('a')
    // JS automatically converts the input (64bit INT) to '9223372036854776000' during conversion as this is the max value in JS.
    // However the max value received from server is '9223372036854775807'
    // Therefore an explicit replacement is necessary in the JSON content in the file.
    let resJson = JSON.stringify(this.conv).replace(/9223372036854776000/g, '9223372036854775807')
    a.href = 'data:text/json;charset=utf-8,' + encodeURIComponent(resJson)
    a.download = `${this.conv.SessionName}_${this.conv.DatabaseType}_${this.conv.DatabaseName}.json`
    a.click()
  }
  
  downloadArtifacts(){
    this.downloadStructuredReport()
    this.downloadSession()
    this.downloadTextReport()
    this.downloadDDL()
  }

  // downloads structured report of the migration in JSON format
  downloadStructuredReport(){
    var a = document.createElement('a')
    this.fetch.getDStructuredReport().subscribe({ 
      next: (res: IStructuredReport) => {
        let resJson = JSON.stringify(res).replace(/9223372036854776000/g, '9223372036854775807')
        a.href = 'data:text/json;charset=utf-8,' + encodeURIComponent(resJson)
        a.download = `${this.conv.DatabaseName}_migration_structuredReport.json`
        a.click()
      }
    })
  }

  // downloads text report of the migration in text format in more human readable form
  downloadTextReport(){
    var a = document.createElement('a')
    this.fetch.getDTextReport().subscribe({  
      next: (res: string) => {
        a.href = 'data:text;charset=utf-8,' + encodeURIComponent(res)
        a.download = `${this.conv.DatabaseName}_migration_textReport.txt`
        a.click()
      }
    })
  }

  // downloads text file of Spanner's DDL of the schema. However this is optimized for reading and includes comments, foreign keys
  // and doesn't add backticks around table and column names. This is not strictly
	// legal Cloud Spanner DDL (Cloud Spanner doesn't currently support comments).
  downloadDDL(){
    var a = document.createElement('a')
    this.fetch.getDSpannerDDL().subscribe({  
      next: (res: string) => {
        a.href = 'data:text;charset=utf-8,' + encodeURIComponent(res)
        a.download = `${this.conv.DatabaseName}_spannerDDL.txt`
        a.click()
      }
    })
  }

  updateSpannerTable(data: IUpdateTableArgument) {
    this.spannerTree = this.conversion.createTreeNode(
      this.conv,
      this.conversionRates,
      data.text,
      data.order
    )
  }

  updateSrcTable(data: IUpdateTableArgument) {
    this.srcTree = this.conversion.createTreeNodeForSource(
      this.conv,
      this.conversionRates,
      data.text,
      data.order
    )
  }

  isIndexAddedOrRemoved(data: IConv) {
    if (this.conv) {
      let prevIndexCount = 0
      let curIndexCount = 0
      Object.entries(this.conv.SpSchema).forEach((item) => {
        prevIndexCount += item[1].Indexes ? item[1].Indexes.length : 0
      })
      Object.entries(data.SpSchema).forEach((item) => {
        curIndexCount += item[1].Indexes ? item[1].Indexes.length : 0
      })
      if (prevIndexCount !== curIndexCount) return true
      else return false
    }
    return false
  }
  prepareMigration() {
    this.fetch.getTableWithErrors().subscribe({
      next: (res: ITableIdAndName[]) => {
        if (res != null && res.length !=0)
        {
          console.log(res.map(x => x.Name).join(', '));
          let errMsg = 'Please fix the errors for the following tables to move ahead: '+ res.map(x => x.Name).join(', ')
          this.dialog.open(InfodialogComponent, {
            data: { message: errMsg, type: 'error', title: 'Error in Spanner Draft' },
            maxWidth: '500px',
          })
        } else if (this.isOfflineStatus) {
          this.dialog.open(InfodialogComponent, {
            data: { message: "Please configure spanner project id and instance id to proceed", type: 'error', title: 'Configure Spanner' },
            maxWidth: '500px',
          })
        } else {
          this.router.navigate(['/prepare-migration'])
        }
      }
    }) 
  }
  spannerTab() {
    this.clickEvent.setTabToSpanner()
  }
}
