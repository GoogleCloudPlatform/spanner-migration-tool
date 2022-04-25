import { Component, OnDestroy, OnInit } from '@angular/core'
import { DataService } from 'src/app/services/data/data.service'
import { ConversionService } from '../../services/conversion/conversion.service'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'
import IConv from '../../model/Conv'
import { Subscription } from 'rxjs/internal/Subscription'
import { MatDialog } from '@angular/material/dialog'
import { SaveSessionFormComponent } from '../save-session-form/save-session-form.component'
import IFkTabData from 'src/app/model/FkTabData'
import IColumnTabData, { IIndexData } from '../../model/EditTable'
import ISchemaObjectNode, { FlatNode } from 'src/app/model/SchemaObjectNode'
import { ObjectExplorerNodeType } from 'src/app/app.constants'

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
  ddlStmts: any
  isOfflineStatus: boolean = false
  spannerTree: ISchemaObjectNode[] = []
  srcTree: ISchemaObjectNode[] = []
  constructor(
    private data: DataService,
    private conversion: ConversionService,
    private dialog: MatDialog,
    private sidenav: SidenavService
  ) {
    this.currentObject = null
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
      if (this.currentObject && this.currentObject.type === ObjectExplorerNodeType.Table) {
        this.fkData = this.currentObject
          ? this.conversion.getFkMapping(this.currentObject.name, data)
          : []

        this.tableData = this.currentObject
          ? this.conversion.getColumnMapping(this.currentObject.name, data)
          : []
      }
    })

    this.converObj = this.data.conversionRate.subscribe((rates: any) => {
      this.conversionRates = rates
      this.reRenderObjectExplorerSpanner()
      this.reRenderObjectExplorerSrc()
    })

    this.data.isOffline.subscribe({
      next: (res: boolean) => {
        this.isOfflineStatus = res
      },
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
    if (object.type === ObjectExplorerNodeType.Table) {
      this.currentObject = object
      this.tableData = this.currentObject
        ? this.conversion.getColumnMapping(this.currentObject.name, this.conv)
        : []

      this.fkData = []
      this.fkData = this.currentObject
        ? this.conversion.getFkMapping(this.currentObject.name, this.conv)
        : []
    } else {
      this.currentObject = object
      this.indexData = this.conversion.getIndexMapping(object.parent, this.conv, object.name)
      console.log(this.indexData)
    }
  }

  leftColumnToggle() {
    this.isLeftColumnCollapse = !this.isLeftColumnCollapse
  }

  rightColumnToggle() {
    this.isRightColumnCollapse = !this.isRightColumnCollapse
  }

  openAssessment() {
    this.sidenav.openSidenav()
    this.sidenav.setSidenavComponent('assessment')
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

  searchSpannerTable(text: string) {
    this.spannerTree = this.conversion.createTreeNode(this.conv, this.conversionRates, text)
  }

  searchSrcTable(text: string) {
    this.srcTree = this.conversion.createTreeNodeForSource(this.conv, this.conversionRates, text)
  }
}
