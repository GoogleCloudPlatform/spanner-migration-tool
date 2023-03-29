import { Injectable } from '@angular/core'
import { BehaviorSubject } from 'rxjs'
import IRule from 'src/app/model/rule'

@Injectable({
  providedIn: 'root',
})
export class SidenavService {
  private sidenavOpenSub = new BehaviorSubject<boolean>(false)
  private sidenavComponentSub = new BehaviorSubject<string>('')
  private sidenavRuleTypeSub = new BehaviorSubject<string>('')
  private sidenavAddIndexTableSub = new BehaviorSubject<string>('')
  private setSidenavDatabaseNameSub = new BehaviorSubject<string>('')
  private ruleDataSub = new BehaviorSubject<IRule>({})
  private displayRuleFlagSub = new BehaviorSubject<boolean>(false)
  private setMiddleColumn = new BehaviorSubject<any>(false)
  constructor() {}
  isSidenav = this.sidenavOpenSub.asObservable()
  sidenavComponent = this.sidenavComponentSub.asObservable()
  sidenavRuleType = this.sidenavRuleTypeSub.asObservable()
  sidenavAddIndexTable = this.sidenavAddIndexTableSub.asObservable()
  sidenavDatabaseName = this.setSidenavDatabaseNameSub.asObservable()
  ruleData = this.ruleDataSub.asObservable()
  displayRuleFlag = this.displayRuleFlagSub.asObservable()
  setMiddleColumnComponent = this.setMiddleColumn.asObservable()

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
  setSidenavDatabaseName(DatabaseName: string) {
    this.setSidenavDatabaseNameSub.next(DatabaseName)
  }
  setRuleData(data: any) {
    this.ruleDataSub.next(data)
  }
  setDisplayRuleFlag(flag: boolean) {
    this.displayRuleFlagSub.next(flag)
  }

  setMiddleColComponent(flag: boolean) {
    this.setMiddleColumn.next(flag)
  }
}
