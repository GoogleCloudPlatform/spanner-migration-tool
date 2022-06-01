import { Injectable } from '@angular/core'
import { BehaviorSubject } from 'rxjs'

@Injectable({
  providedIn: 'root',
})
export class SidenavService {
  private sidenavOpenSub = new BehaviorSubject<boolean>(false)
  private sidenavComponentSub = new BehaviorSubject<string>('')
  private sidenavRuleTypeSub = new BehaviorSubject<string>('')
  private sidenavAddIndexTableSub = new BehaviorSubject<string>('')
  constructor() {}
  isSidenav = this.sidenavOpenSub.asObservable()
  sidenavComponent = this.sidenavComponentSub.asObservable()
  sidenavRuleType = this.sidenavRuleTypeSub.asObservable()
  sidenavAddIndexTable = this.sidenavAddIndexTableSub.asObservable()

  openSidenav() {
    this.sidenavOpenSub.next(true)
  }

  closeSidenav() {
    this.sidenavOpenSub.next(false)
  }
  setSidenavComponent(data: string) {
    this.sidenavComponentSub.next(data)
  }
  setSidenavRuleType(type: string) {
    this.sidenavRuleTypeSub.next(type)
  }
  setSidenavAddIndexTable(tableName: string) {
    this.sidenavAddIndexTableSub.next(tableName)
  }
}
