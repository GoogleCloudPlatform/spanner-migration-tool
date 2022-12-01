import { Component, HostListener } from '@angular/core'
import { SidenavService } from './services/sidenav/sidenav.service'

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss'],
})
export class AppComponent {
  title = 'ui'
  showSidenav: boolean = false
  sidenavComponent: string = ''
  constructor(private sidenavService: SidenavService) {}
  ngOnInit(): void {
    localStorage.clear()
    this.sidenavService.isSidenav.subscribe((data) => {
      this.showSidenav = data
    })
    this.sidenavService.sidenavComponent.subscribe((data) => {
      this.sidenavComponent = data
    })
  }
  closeSidenav(): void {
    this.showSidenav = false
  }
}
