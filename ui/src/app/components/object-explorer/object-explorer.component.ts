import { Component, EventEmitter, Input, OnInit, Output, SimpleChanges } from '@angular/core'
import ISchemaObjectNode, { FlatNode } from 'src/app/model/schema-object-node'
import { FlatTreeControl } from '@angular/cdk/tree'
import { MatTreeFlatDataSource, MatTreeFlattener } from '@angular/material/tree'
import { ConversionService } from '../../services/conversion/conversion.service'
import { ObjectExplorerNodeType, StorageKeys } from 'src/app/app.constants'
import { SidenavService } from '../../services/sidenav/sidenav.service'
import { IUpdateTableArgument } from 'src/app/model/update-table'
import IConv from '../../model/conv'
import { ClickEventService } from 'src/app/services/click-event/click-event.service'

@Component({
  selector: 'app-object-explorer',
  templateUrl: './object-explorer.component.html',
  styleUrls: ['./object-explorer.component.scss'],
})
export class ObjectExplorerComponent implements OnInit {
  conv!: IConv
  isLeftColumnCollapse: boolean = false
  currentSelectedObject: FlatNode | null = null
  srcSortOrder: string = ''
  spannerSortOrder: string = ''
  srcSearchText: string = ''
  spannerSearchText: string = ''
  selectedTab: string = 'spanner'
  @Output() selectedDatabase = new EventEmitter<string>()
  @Output() selectObject = new EventEmitter<FlatNode>()
  @Output() updateSpannerTable = new EventEmitter<IUpdateTableArgument>()
  @Output() updateSrcTable = new EventEmitter<IUpdateTableArgument>()
  @Output() leftCollaspe: EventEmitter<any> = new EventEmitter()
  @Input() spannerTree: ISchemaObjectNode[] = []
  @Input() srcTree: ISchemaObjectNode[] = []
  @Input() srcDbName: string = ''
  selectedIndex: number = 1

  private transformer = (node: ISchemaObjectNode, level: number) => {
    return {
      expandable: !!node.children && node.children.length > 0,
      name: node.name,
      status: node.status,
      type: node.type,
      parent: node.parent,
      pos: node.pos,
      isSpannerNode: node.isSpannerNode,
      level: level,
      isDeleted: node.isDeleted ? true : false,
      id: node.id,
      parentId: node.parentId,
    }
  }
  treeControl = new FlatTreeControl<FlatNode>(
    (node) => node.level,
    (node) => node.expandable
  )

  srcTreeControl = new FlatTreeControl<FlatNode>(
    (node) => node.level,
    (node) => node.expandable
  )

  treeFlattener = new MatTreeFlattener(
    this.transformer,
    (node) => node.level,
    (node) => node.expandable,
    (node) => node.children
  )
  dataSource = new MatTreeFlatDataSource(this.treeControl, this.treeFlattener)
  srcDataSource = new MatTreeFlatDataSource(this.srcTreeControl, this.treeFlattener)

  displayedColumns: string[] = ['status', 'name']

  constructor(
    private conversion: ConversionService,
    private sidenav: SidenavService,
    private clickEvent: ClickEventService
  ) {}

  ngOnInit(): void {
    this.clickEvent.tabToSpanner.subscribe({
      next: (res: boolean) => {
        this.setSpannerTab()
      },
    })
  }

  ngOnChanges(changes: SimpleChanges): void {
    let newSpannerTree = changes?.['spannerTree']?.currentValue
    let newSrcTree = changes?.['srcTree']?.currentValue

    if (newSrcTree) {
      this.srcDataSource.data = newSrcTree
      this.srcTreeControl.expand(this.srcTreeControl.dataNodes[0])
      this.srcTreeControl.expand(this.srcTreeControl.dataNodes[1])
    }
    if (newSpannerTree) {
      this.dataSource.data = newSpannerTree
      this.treeControl.expand(this.treeControl.dataNodes[0])
      this.treeControl.expand(this.treeControl.dataNodes[1])
    }
  }

  filterSpannerTable() {
    this.updateSpannerTable.emit({ text: this.spannerSearchText, order: this.spannerSortOrder })
  }

  filterSrcTable() {
    this.updateSrcTable.emit({ text: this.srcSearchText, order: this.srcSortOrder })
  }

  srcTableSort() {
    if (this.srcSortOrder === '') {
      this.srcSortOrder = 'asc'
    } else if (this.srcSortOrder === 'asc') {
      this.srcSortOrder = 'desc'
    } else {
      this.srcSortOrder = ''
    }
    this.updateSrcTable.emit({ text: this.srcSearchText, order: this.srcSortOrder })
  }
  spannerTableSort() {
    if (this.spannerSortOrder === '') {
      this.spannerSortOrder = 'asc'
    } else if (this.spannerSortOrder === 'asc') {
      this.spannerSortOrder = 'desc'
    } else {
      this.spannerSortOrder = ''
    }
    this.updateSpannerTable.emit({ text: this.spannerSearchText, order: this.spannerSortOrder })
  }

  objectSelected(data: FlatNode) {
    this.currentSelectedObject = data
    if (data.type === ObjectExplorerNodeType.Index || data.type === ObjectExplorerNodeType.Table) {
      this.selectObject.emit(data)
    }
  }

  leftColumnToggle() {
    this.isLeftColumnCollapse = !this.isLeftColumnCollapse
    this.leftCollaspe.emit()
  }

  isTableNode(name: string) {
    return new RegExp('^tables').test(name)
  }

  isIndexNode(name: string) {
    return new RegExp('^indexes').test(name)
  }

  openAddIndexForm(tableName: string): void {
    this.sidenav.setSidenavAddIndexTable(tableName)
    this.sidenav.setSidenavRuleType('addIndex')
    this.sidenav.openSidenav()
    this.sidenav.setSidenavComponent('rule')
  }

  shouldHighlight(data: FlatNode) {
    if (
      data.name === this.currentSelectedObject?.name &&
      (data.type === ObjectExplorerNodeType.Table || data.type === ObjectExplorerNodeType.Index)
    ) {
      return true
    } else {
      return false
    }
  }
  onTabChanged() {
    if (this.selectedTab == 'spanner') {
      this.selectedTab = 'source'
      this.selectedIndex = 0
    } else {
      this.selectedTab = 'spanner'
      this.selectedIndex = 1
    }
    this.selectedDatabase.emit(this.selectedTab)
    this.currentSelectedObject = null
    this.selectObject.emit(undefined)
  }
  setSpannerTab() {
    this.selectedIndex = 1
  }
}
