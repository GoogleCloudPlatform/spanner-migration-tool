import { Component, EventEmitter, Input, OnInit, Output, SimpleChanges } from '@angular/core'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'
import { DataService } from 'src/app/services/data/data.service'

@Component({
  selector: 'app-rule',
  templateUrl: './rule.component.html',
  styleUrls: ['./rule.component.scss'],
})
export class RuleComponent implements OnInit {
  dataSource: any = []
  displayedColumns = ['order', 'name', 'type', 'objectType', 'associatedObject', 'enabled']
  @Input() currentObject: any = {}
  @Output() lengthOfRules: EventEmitter<number> = new EventEmitter<number>()

  constructor(private sidenavService: SidenavService, private data: DataService) {}

  ngOnInit(): void {
    this.dataSource = []
  }

  ngOnChanges(changes: SimpleChanges): void {
    let currentData: any = []

    this.data.rule.subscribe({
      next: (data: any) => {
        this.dataSource = data
      },
    })

    if (this.currentObject.type === 'tableName' || this.currentObject.type === 'indexName') {
      currentData = this.dataSource.filter(
        (index: any) =>
          index.AssociatedObjects === this.currentObject.name ||
          index.AssociatedObjects === this.currentObject.parent ||
          index.Type === 'global_datatype_change'
      )
    }

    this.dataSource = currentData
    this.lengthOfRules.emit(currentData.length)
  }

  openSidenav(): void {
    this.sidenavService.openSidenav()
    this.sidenavService.setSidenavComponent('rule')
  }
}
