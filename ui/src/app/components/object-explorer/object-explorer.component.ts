import { Component, EventEmitter, Input, OnInit, Output, SimpleChanges } from '@angular/core'
import ISchemaObjectNode, { FlatNode } from 'src/app/model/SchemaObjectNode'
import { FlatTreeControl } from '@angular/cdk/tree'
import { MatTreeFlatDataSource, MatTreeFlattener } from '@angular/material/tree'
import { ConversionService } from '../../services/conversion/conversion.service'
import { ObjectExplorerNodeType, StorageKeys } from 'src/app/app.constants'
import { SidenavService } from '../../services/sidenav/sidenav.service'

@Component({
  selector: 'app-object-explorer',
  templateUrl: './object-explorer.component.html',
  styleUrls: ['./object-explorer.component.scss'],
})
export class ObjectExplorerComponent implements OnInit {
  searchText: string = ''
  isLeftColumnCollapse: boolean = false
  srcDbName: string = localStorage.getItem(StorageKeys.SourceDbName) as string
  currentSelectedObject: FlatNode | null = null
  @Output() selectObject = new EventEmitter<FlatNode>()
  @Output() searchSpannerTable = new EventEmitter<string>()
  @Output() searchSrcTable = new EventEmitter<string>()
  @Output() leftCollaspe: EventEmitter<any> = new EventEmitter()
  @Input() spannerTree: ISchemaObjectNode[] = []
  @Input() srcTree: ISchemaObjectNode[] = []

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

  constructor(private conversion: ConversionService, private sidenav: SidenavService) {}

  ngOnInit(): void {
    // this.treeControl.dataNodes = this.dataSource.data.map((d) => this.transformer(d, 1))
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

  filterSpannerTable(text: string) {
    this.searchSpannerTable.emit(text)
  }

  filterSrcTable(text: string) {
    this.searchSrcTable.emit(text)
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

  openSidenav(): void {
    this.sidenav.openSidenav()
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
}
