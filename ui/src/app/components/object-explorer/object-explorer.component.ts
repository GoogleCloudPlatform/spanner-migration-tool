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
import { SelectionModel } from '@angular/cdk/collections'
import { DataService } from 'src/app/services/data/data.service'
import { take } from 'rxjs'
import { ITableState, ITables } from 'src/app/model/migrate'
import { MatDialog } from '@angular/material/dialog'
import { BulkDropRestoreTableDialogComponent } from '../bulk-drop-restore-table-dialog/bulk-drop-restore-table-dialog.component'

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
  @Output() updateSidebar = new EventEmitter<boolean>()
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
      parentId: node.parentId,
      pos: node.pos,
      isSpannerNode: node.isSpannerNode,
      level: level,
      isDeleted: node.isDeleted ? true : false,
      id: node.id,
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
  checklistSelection = new SelectionModel<FlatNode>(true, []);

  displayedColumns: string[] = ['status', 'name']

  constructor(
    private conversion: ConversionService,
    private dialog: MatDialog,
    private data: DataService,
    private sidenav: SidenavService,
    private clickEvent: ClickEventService
  ) { }

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

  isIndexLikeNode(data: FlatNode): boolean {
    if (data.type == ObjectExplorerNodeType.Index || data.type == ObjectExplorerNodeType.Indexes) {
      return true
    }
    return false
  }

  openAddIndexForm(tableName: string): void {
    this.sidenav.setSidenavAddIndexTable(tableName)
    this.sidenav.setSidenavRuleType('addIndex')
    this.sidenav.openSidenav()
    this.sidenav.setSidenavComponent('rule')
    this.sidenav.setRuleData([])
    this.sidenav.setDisplayRuleFlag(false)
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

  checkDropSelection(): boolean {
    let countByCategory = this.countSelectionByCategory()
    if (countByCategory.eligibleForDrop != 0) {
      return true
    }
    return false
  }

  checkRestoreSelection(): boolean {
    let countByCategory = this.countSelectionByCategory()
    if (countByCategory.eligibleForRestore != 0) {
      return true
    }
    return false
  }

  countSelectionByCategory(): {eligibleForDrop: number, eligibleForRestore: number} {
    const values = this.checklistSelection.selected
    let eligibleForDrop = 0
    let eligibleForRestore = 0
    values.forEach((flatNode) => {
      if (!flatNode.isDeleted) {
        eligibleForDrop += 1
      } else {
        eligibleForRestore += 1
      }
    })
    return {eligibleForDrop: eligibleForDrop, eligibleForRestore: eligibleForRestore}
  }

  dropSelected() {
    //selected values to be dropped
    const values = this.checklistSelection.selected
    var tablesWithState: ITableState[] = []
    
    values.forEach((flatNode) => {
      if (flatNode.id != "" && flatNode.type == ObjectExplorerNodeType.Table) {
        let table: ITableState = {
          TableId: flatNode.id,
          TableName: flatNode.name,
          isDeleted: flatNode.isDeleted
        }
        tablesWithState.push(table)
      }
    })
    //confirm from the user
    let openDialog = this.dialog.open(BulkDropRestoreTableDialogComponent, {
      width: '35vw',
      minWidth: '450px',
      maxWidth: '600px',
      maxHeight: '90vh',
      data: { tables: tablesWithState, operation: 'SKIP' },
    })
    //state is ephimeral (UI only), and name is not relevant to the API.
    var tables: ITables = {
      TableList: []
    };
    tablesWithState.forEach((tableWithState) => {
      tables.TableList.push(tableWithState.TableId)
    })
    //upon confirmation, delete
    openDialog.afterClosed().subscribe((res: string) => {
      if (res == 'SKIP') {
        this.data.dropTables(tables)
          .pipe(take(1))
          .subscribe((res: string) => {
            if (res === '') {
              this.data.getConversionRate()
              this.updateSidebar.emit(true)
            }
          })
          //clear selection after a successful operation
          this.checklistSelection.clear()
      }
    })
  }

  restoreSelected() {
    //selected values to be restored
    const values = this.checklistSelection.selected
    var tablesWithState: ITableState[] = []
    
    values.forEach((flatNode) => {
      if (flatNode.id != "" && flatNode.type == ObjectExplorerNodeType.Table) {
        let table: ITableState = {
          TableId: flatNode.id,
          TableName: flatNode.name,
          isDeleted: flatNode.isDeleted
        }
        tablesWithState.push(table)
      }
    })
    //confirm from the user
    let openDialog = this.dialog.open(BulkDropRestoreTableDialogComponent, {
      width: '35vw',
      minWidth: '450px',
      maxWidth: '600px',
      data: { tables: tablesWithState, operation: 'RESTORE' },
    })
    //state is ephimeral (UI only), and name is not relevant to the API.
    var tables: ITables = {
      TableList: []
    };
    tablesWithState.forEach((tableWithState) => {
      tables.TableList.push(tableWithState.TableId)
    })
    //upon confirmation, restore
    openDialog.afterClosed().subscribe((res: string) => {
      if (res == 'RESTORE') {
        this.data
          .restoreTables(tables)
          .pipe(take(1))
          .subscribe((res: string) => {
            if (res === '') {
            }
            this.data.getConversionRate()
            this.data.getDdl()
          })
          //clear selection after a successful operation
          this.checklistSelection.clear()
      }
    })
  }

  /** Toggle the to-do item selection. Select/deselect all the descendants node */
  selectionToggle(node: FlatNode): void {
    this.checklistSelection.toggle(node);
    const descendants = this.treeControl.getDescendants(node);
    this.checklistSelection.isSelected(node)
      ? this.checklistSelection.select(...descendants)
      : this.checklistSelection.deselect(...descendants);
  }
}
