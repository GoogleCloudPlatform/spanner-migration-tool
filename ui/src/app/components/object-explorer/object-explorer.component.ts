import { Component, EventEmitter, OnInit, Output } from '@angular/core'
import ISchemaObjectNode from 'src/app/model/SchemaObjectNode'
import { DataService } from '../../services/data/data.service'
import { ConversionService } from '../../services/conversion/conversion.service'
import { NestedTreeControl } from '@angular/cdk/tree'
import { MatTreeNestedDataSource } from '@angular/material/tree'

@Component({
  selector: 'app-object-explorer',
  templateUrl: './object-explorer.component.html',
  styleUrls: ['./object-explorer.component.scss'],
})
export class ObjectExplorerComponent implements OnInit {
  treeControl = new NestedTreeControl<ISchemaObjectNode>((p) => p.children)
  dataSource = new MatTreeNestedDataSource<ISchemaObjectNode>()
  searchText!: string
  tableData!: any
  @Output() selectTable = new EventEmitter<string>()
  constructor(private data: DataService, private convert: ConversionService) {}
  treeData: ISchemaObjectNode[] = [
    {
      name: 'Database Name',
      helth: 'ORANGE',
      children: [
        {
          name: 'Tables',
        },
      ],
    },
  ]

  hasChild = (_: number, node: ISchemaObjectNode) => !!node.children && node.children.length > 0

  ngOnInit(): void {
    this.data.conv.subscribe((data) => {
      this.tableData = data
      this.treeData[0].children![0].children = data.spTables.map((name: string) => {
        return { name: name, helth: this.tableData['colorCode'][name] }
      })
      this.dataSource.data = this.treeData
      this.treeControl.dataNodes = this.dataSource.data
      this.treeControl.expandAll()
    })
  }

  filterTable(text: any) {
    console.log(this.tableData.spTables)
    this.treeData = [
      {
        name: 'Database Name',
        helth: 'ORANGE',
        children: [
          {
            name: 'Tables',
            children: this.tableData.spTables
              .filter((name: string) => name.toLocaleLowerCase().includes(text.toLocaleLowerCase()))
              .map((name: string) => {
                return { name: name, helth: this.tableData['colorCode'][name] }
              }) as ISchemaObjectNode[],
          },
        ],
      },
    ]
    this.dataSource.data = this.treeData
    this.treeControl.dataNodes = this.dataSource.data
    this.treeControl.expandAll()
  }

  tableSelected(e: any) {
    let tname = e.target.textContent.trim()
    this.selectTable.emit(tname)
  }
}
