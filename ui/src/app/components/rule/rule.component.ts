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
  displayedColumns = [
    'order',
    'name',
    'type',
    'objectType',
    'associatedObject',
    'enabled',
    'view',
    'delete',
  ]
  @Input() currentObject: any = {}
  @Output() lengthOfRules: EventEmitter<number> = new EventEmitter<number>()
  @Output() currentRules: EventEmitter<any> = new EventEmitter<any>()

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
      (index: any) => index.Type === 'global_datatype_change'
    )

    if (
      this.currentObject &&
      (this.currentObject?.type === 'tableName' || this.currentObject?.type === 'indexName')
    ) {
      currentData = this.currentDataSource.filter(
        (index: any) =>
          index.AssociatedObjects === this.currentObject.name ||
          index.AssociatedObjects === this.currentObject.parent
      )
    }

    this.dataSource = [...globalData, ...currentData]
    this.currentRules.emit(this.dataSource)
    this.lengthOfRules.emit(this.dataSource.length)
  }

  openSidenav(): void {
    this.sidenavService.openSidenav()
    this.sidenavService.setSidenavComponent('rule')
  }

  viewSidenavRule(): void {
    this.sidenavService.openSidenav()
    this.sidenavService.setSidenavComponent('rule')
  }
}
