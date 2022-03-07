import { Component, Input, OnInit, SimpleChanges } from '@angular/core'
import { MatTreeNestedDataSource } from '@angular/material/tree'
import { NestedTreeControl } from '@angular/cdk/tree'
import ISchemaObjectNode from 'src/app/model/SchemaObjectNode'

@Component({
  selector: 'app-tree-view',
  templateUrl: './tree-view.component.html',
  styleUrls: ['./tree-view.component.scss'],
})
export class TreeViewComponent implements OnInit {
  treeControl = new NestedTreeControl<ISchemaObjectNode>((p) => p.children)
  dataSource = new MatTreeNestedDataSource<ISchemaObjectNode>()
  @Input('data') data: ISchemaObjectNode[] = []
  constructor() {}

  ngOnInit(): void {}

  ngOnChanges(changes: SimpleChanges): void {
    if (!changes?.['data'].firstChange) {
      this.dataSource.data = [
        {
          name: 'Database Name',
          helth: 'ORANGE',
          children: this.data,
        },
      ]
      this.treeControl.dataNodes = this.dataSource.data
      this.treeControl.expandAll()
    }
  }

  hasChild = (_: number, node: ISchemaObjectNode) => !!node.children && node.children.length > 0
}
