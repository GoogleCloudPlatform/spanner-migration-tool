import { Injectable } from '@angular/core'
import { BehaviorSubject } from 'rxjs'

@Injectable({
  providedIn: 'root',
})
export class SidenavService {
  private isSidenavOpen = new BehaviorSubject<boolean>(false)
  constructor() {}
  isSidenav = this.isSidenavOpen.asObservable()

  openSidenav() {
    this.isSidenavOpen.next(true)
  }

  closeSidenav() {
    this.isSidenavOpen.next(false)
  }
}
