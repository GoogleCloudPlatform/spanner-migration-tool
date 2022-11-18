import { Component, OnInit, SimpleChanges } from '@angular/core'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'
import { DataService } from 'src/app/services/data/data.service'
import IRuleContent, { IRule } from 'src/app/model/rule'

@Component({
  selector: 'app-rule',
  templateUrl: './rule.component.html',
  styleUrls: ['./rule.component.scss'],
})
export class RuleComponent implements OnInit {
  dataSource: any = []
  displayedColumns = ['order', 'name', 'type', 'objectType', 'associatedObject', 'enabled']

  constructor(private sidenavService: SidenavService, private data: DataService) {}

  ngOnInit(): void {
    this.data.rule.subscribe({
      next: (data: any) => {
        this.dataSource = data
      },
    })
  }
  ngOnChanges(changes: SimpleChanges): void {
    this.dataSource = changes['dataSource']?.currentValue || this.dataSource
  }
  openSidenav(): void {
    this.sidenavService.openSidenav()
    this.sidenavService.setSidenavComponent('rule')
  }
}
