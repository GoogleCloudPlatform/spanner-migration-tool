import { Component } from '@angular/core'
import { SidenavService } from './services/sidenav/sidenav.service'

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss'],
})
export class AppComponent {
  title = 'ui'
  showSidenav: boolean = false
  constructor(private sidenavService: SidenavService) {}

  ngOnInit(): void {
    this.sidenavService.isSidenav.subscribe((data) => {
      this.showSidenav = data
    })
  }
  closeSidenav(): void {
    this.showSidenav = false
  }
}
