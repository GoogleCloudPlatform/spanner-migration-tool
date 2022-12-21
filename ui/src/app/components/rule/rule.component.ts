import { Component, EventEmitter, Input, OnInit, Output, SimpleChanges } from '@angular/core'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'
import { DataService } from 'src/app/services/data/data.service'
import { FlatNode } from 'src/app/model/schema-object-node'

@Component({
  selector: 'app-rule',
  templateUrl: './rule.component.html',
  styleUrls: ['./rule.component.scss'],
})
export class RuleComponent implements OnInit {
  dataSource: any = []
  currentDataSource: any = []
  displayedColumns = ['order', 'name', 'type', 'objectType', 'associatedObject', 'enabled', 'view']
  @Input() currentObject: FlatNode | null = null
  @Output() lengthOfRules: EventEmitter<number> = new EventEmitter<number>()

  constructor(private sidenavService: SidenavService, private data: DataService) {}

  ngOnInit(): void {
    this.dataSource = []
    this.data.rule.subscribe({
      next: (data: any) => {
        this.currentDataSource = data
        this.updateRules()
      },
    })
  }

  ngOnChanges(): void {
    this.updateRules()
  }

  updateRules(): void {
    if (this.currentDataSource) {
      let globalData: any = []
      let currentData: any = []
      globalData = this.currentDataSource.filter(
        (rule: any) => rule?.Type === 'global_datatype_change'
      )
      if (
        this.currentObject &&
        (this.currentObject?.type === 'tableName' || this.currentObject?.type === 'indexName')
      ) {
        currentData = this.currentDataSource
          .filter(
            (rule: any) =>
              rule?.AssociatedObjects === this.currentObject?.id ||
              rule?.AssociatedObjects === this.currentObject?.parentId ||
              rule?.AssociatedObjects === this.currentObject?.name ||
              rule?.AssociatedObjects === this.currentObject?.parent
          )
          .map((rule: any) => {
            let tableName: string = ''
            if (this.currentObject?.type === 'tableName') {
              tableName = this.currentObject.name
            } else if (this.currentObject?.type === 'indexName') {
              tableName = this.currentObject.parent
            }
            rule.AssociatedObjects = tableName
            return rule
          })
      }
      this.dataSource = [...globalData, ...currentData]
      this.lengthOfRules.emit(this.dataSource.length)
    } else {
      this.dataSource = []
      this.lengthOfRules.emit(0)
    }
  }

  openSidenav(): void {
    this.sidenavService.openSidenav()
    this.sidenavService.setSidenavComponent('rule')
    this.sidenavService.passRule([], false)
  }

  viewSidenavRule(Id: any): void {
    let selectedRule: any = []
    for (let i = 0; i < this.dataSource.length; i++) {
      if (this.dataSource[i].Id === Id) {
        selectedRule = this.dataSource[i]
        break
      }
    }

    this.sidenavService.openSidenav()
    this.sidenavService.setSidenavComponent('rule')
    this.sidenavService.passRule(selectedRule, true)
  }
}
