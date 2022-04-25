import { Component, OnInit } from '@angular/core'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'

@Component({
  selector: 'app-sidenav-view-assessment',
  templateUrl: './sidenav-view-assessment.component.html',
  styleUrls: ['./sidenav-view-assessment.component.scss'],
})
export class SidenavViewAssessmentComponent implements OnInit {
  constructor(private sidenav: SidenavService) {}

  ngOnInit(): void {}

  closeSidenav() {
    this.sidenav.closeSidenav()
  }
}
