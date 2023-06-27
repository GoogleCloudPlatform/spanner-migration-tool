import { Component, EventEmitter, Input, OnInit, Output, SimpleChanges } from '@angular/core'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'
import { DataService } from 'src/app/services/data/data.service'
import { FlatNode } from 'src/app/model/schema-object-node'
import { ITransformation } from 'src/app/model/rule'

@Component({
  selector: 'app-rule',
  templateUrl: './rule.component.html',
  styleUrls: ['./rule.component.scss'],
})
export class RuleComponent implements OnInit {
  dataSource: any = []
  currentDataSource: any = []
  transformations: ITransformation[] = []
  displayedColumns = ['order', 'name', 'type', 'objectType', 'associatedObject', 'enabled', 'view']
  @Input() currentObject: FlatNode | null = null
  @Output() lengthOfRules: EventEmitter<number> = new EventEmitter<number>()

  constructor(private sidenavService: SidenavService, private data: DataService) { }

  ngOnInit(): void {
    this.dataSource = []
    this.data.rule.subscribe({
      next: (data: any) => {
        this.currentDataSource = data
        this.data.transformation.subscribe({
          next: (data: any) => {
            this.transformations = data
            this.updateRules()
          },
        })
      },
    })

  }

  ngOnChanges(): void {
    this.updateRules()
  }

  updateRules(): void {
    if (this.currentDataSource || this.transformations) {
      if (this.currentDataSource) {
        let globalData: any = []
        let currentData: any = []
        globalData = this.currentDataSource.filter(
          (rule: any) => rule?.Type === 'global_datatype_change' ||
            (rule?.Type === 'edit_column_max_length' && rule?.AssociatedObjects === 'All table')
        )
        if (
          this.currentObject &&
          (this.currentObject?.type === 'tableName' || this.currentObject?.type === 'indexName')
        ) {
          currentData = this.filterForCurrentObject(this.currentDataSource)
        }
        this.dataSource = [...globalData, ...currentData]
      }
      if (this.transformations) {
        let currentData = this.filterForCurrentObject(this.transformations)
        this.dataSource = [...this.dataSource, ...currentData]
      }
    } else { 
      this.dataSource = []
    }
    this.lengthOfRules.emit(this.dataSource.length)
  }

  filterForCurrentObject(currentDataSource: any) {
    let currentData: any = []
    currentData = currentDataSource
      .filter(
        (rule: any) =>
          rule?.AssociatedObjects === this.currentObject?.id ||
          rule?.AssociatedObjects === this.currentObject?.parentId ||
          rule?.AssociatedObjects === this.currentObject?.name ||
          rule?.AssociatedObjects === this.currentObject?.parent
      )
      .map((rule: any) => {
        let tableId: string = ''
        if (this.currentObject?.type === 'tableName') {
          tableId = this.currentObject.id
        } else if (this.currentObject?.type === 'indexName') {
          tableId = this.currentObject.parentId
        }
        rule.AssociatedObjects = tableId
        return rule
      })
      return currentData
  }

  openSidenav(): void {
    this.sidenavService.openSidenav()
    this.sidenavService.setSidenavComponent('rule')
    this.sidenavService.setSidenavRuleType('')
    this.sidenavService.setRuleData({})
    this.sidenavService.setDisplayRuleFlag(false)
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
    this.sidenavService.setRuleData(selectedRule)
    this.sidenavService.setDisplayRuleFlag(true)
  }
}
