import { Component, EventEmitter, Input, OnInit, Output, SimpleChanges } from '@angular/core'
import ISchemaObjectNode from 'src/app/model/SchemaObjectNode'
import { FlatTreeControl } from '@angular/cdk/tree'
import { MatTreeFlatDataSource, MatTreeFlattener } from '@angular/material/tree'
import { ConversionService } from '../../services/conversion/conversion.service'
import { ObjectExplorerNodeType, StorageKeys } from 'src/app/app.constants'
import { ThisReceiver } from '@angular/compiler'

interface FlatNode {
  expandable: boolean
  name: string
  status: string | undefined
  level: number
}

@Component({
  selector: 'app-object-explorer',
  templateUrl: './object-explorer.component.html',
  styleUrls: ['./object-explorer.component.scss'],
})
export class ObjectExplorerComponent implements OnInit {
  searchText: string = ''
  isLeftColumnCollapse: boolean = false
  srcDbName: string = localStorage.getItem(StorageKeys.SourceDbName) as string

  @Output() selectTable = new EventEmitter<string>()
  @Output() leftCollaspe: EventEmitter<any> = new EventEmitter()
  @Input() tableNames!: string[]
  @Input() conversionRates!: Record<string, string>

  private transformer = (node: ISchemaObjectNode, level: number) => {
    return {
      expandable: !!node.children && node.children.length > 0,
      name: node.name,
      status: node.status,
      level: level,
    }
  }
  treeControl = new FlatTreeControl<FlatNode>(
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
  displayedColumns: string[] = ['status', 'name']

  constructor(private conversion: ConversionService) {}

  ngOnInit(): void {
    this.treeControl.dataNodes = this.dataSource.data.map((d) => this.transformer(d, 1))
  }

  ngOnChanges(changes: SimpleChanges): void {
    this.tableNames = changes?.['tableNames']?.currentValue || this.tableNames
    this.conversionRates = changes?.['conversionRates']?.currentValue || this.conversionRates
    this.dataSource.data = this.conversion.createTreeNode(this.tableNames, this.conversionRates)
    // this.treeControl.expandDescendants(this.transformer(this.dataSource.data[1], 2))
    this.treeControl.expand(this.treeControl.dataNodes[0])
    this.treeControl.expand(this.treeControl.dataNodes[1])
  }

  filterTable(text: string) {
    this.dataSource.data = this.conversion.createTreeNode(
      this.tableNames.filter((name: string) =>
        name.toLocaleLowerCase().includes(text.toLocaleLowerCase())
      ),
      this.conversionRates
    )
    this.treeControl.expand(this.treeControl.dataNodes[0])
    this.treeControl.expand(this.treeControl.dataNodes[1])
  }

  tableSelected(name: string) {
    this.selectTable.emit(name)
  }

  leftColumnToggle() {
    this.isLeftColumnCollapse = !this.isLeftColumnCollapse
    this.leftCollaspe.emit()
  }

  shouldDisplayTableIcon(name: string) {
    return new RegExp('^Tables').test(name)
  }

  shouldDisplayIndexIcon(name: string) {
    return new RegExp('^Indexes').test(name)
  }
}
