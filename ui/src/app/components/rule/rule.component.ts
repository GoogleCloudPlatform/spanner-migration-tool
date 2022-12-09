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
  currentDataSource: any = []
  displayedColumns = ['order', 'name', 'type', 'objectType', 'associatedObject', 'enabled', 'view']
  @Input() currentObject: any = {}
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
    let globalData: any = []
    let currentData: any = []

    globalData = this.currentDataSource.filter(
      (index: any) => index?.Type === 'global_datatype_change'
    )

    if (
      this.currentObject &&
      (this.currentObject?.type === 'tableName' || this.currentObject?.type === 'indexName')
    ) {
      currentData = this.currentDataSource.filter(
        (index: any) =>
          index?.AssociatedObjects === this.currentObject?.name ||
          index?.AssociatedObjects === this.currentObject?.parent
      )
    }

    this.dataSource = [...globalData, ...currentData]
    this.lengthOfRules.emit(this.dataSource.length)
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
