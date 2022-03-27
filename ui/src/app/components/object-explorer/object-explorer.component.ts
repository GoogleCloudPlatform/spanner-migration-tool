import { Component, EventEmitter, Input, OnInit, Output, SimpleChanges } from '@angular/core'
import ISchemaObjectNode from 'src/app/model/SchemaObjectNode'
import { NestedTreeControl } from '@angular/cdk/tree'
import { MatTreeNestedDataSource } from '@angular/material/tree'
import { ConversionService } from '../../services/conversion/conversion.service'

@Component({
  selector: 'app-object-explorer',
  templateUrl: './object-explorer.component.html',
  styleUrls: ['./object-explorer.component.scss'],
})
export class ObjectExplorerComponent implements OnInit {
  treeControl = new NestedTreeControl<ISchemaObjectNode>((p) => p.children)
  dataSource = new MatTreeNestedDataSource<ISchemaObjectNode>()
  searchText: string = ''
  isLeftColumnCollapse: boolean = false

  @Output() selectTable = new EventEmitter<string>()
  @Output() leftCollaspe: EventEmitter<any> = new EventEmitter()
  constructor(private conversion: ConversionService) {}
  @Input() tableNames!: string[]
  @Input() conversionRates!: Record<string, string>

  hasChild = (_: number, node: ISchemaObjectNode) => !!node.children && node.children.length > 0

  ngOnInit(): void {}

  ngOnChanges(changes: SimpleChanges): void {
    this.tableNames = changes?.['tableNames']?.currentValue || this.tableNames
    this.conversionRates = changes?.['conversionRates']?.currentValue || this.conversionRates
    this.dataSource.data = this.conversion.createTreeNode(this.tableNames, this.conversionRates)
    this.treeControl.dataNodes = this.dataSource.data
    this.treeControl.expandAll()
  }

  filterTable(text: string) {
    this.dataSource.data = this.conversion.createTreeNode(
      this.tableNames.filter((name: string) =>
        name.toLocaleLowerCase().includes(text.toLocaleLowerCase())
      ),
      this.conversionRates
    )
    this.treeControl.dataNodes = this.dataSource.data
    this.treeControl.expandAll()
  }

  tableSelected(e: any) {
    this.selectTable.emit(e.textContent.trim())
  }

  leftColumnToggle() {
    this.isLeftColumnCollapse = !this.isLeftColumnCollapse
    this.leftCollaspe.emit()
  }
}
