import { Component, OnInit } from '@angular/core'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'

@Component({
  selector: 'app-instruction',
  templateUrl: './instruction.component.html',
  styleUrls: ['./instruction.component.scss'],
})
export class InstructionComponent implements OnInit {
  constructor(private sidenav: SidenavService) {}

  ngOnInit(): void {}
  closeInstructionSidenav() {
    this.sidenav.closeSidenav()
  }
}
