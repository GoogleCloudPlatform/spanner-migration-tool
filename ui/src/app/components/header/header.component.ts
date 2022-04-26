import { Component, OnInit } from '@angular/core'
import ISpannerConfig from '../../model/SpannerConfig'
import { MatDialog } from '@angular/material/dialog'
import { UpdateSpannerConfigFormComponent } from '../update-spanner-config-form/update-spanner-config-form.component'
import { DataService } from 'src/app/services/data/data.service'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'

@Component({
  selector: 'app-header',
  templateUrl: './header.component.html',
  styleUrls: ['./header.component.scss'],
})
export class HeaderComponent implements OnInit {
  spannerConfig: ISpannerConfig
  isOfflineStatus: boolean = false
  constructor(
    private data: DataService,
    private dialog: MatDialog,
    private sidenav: SidenavService
  ) {
    this.spannerConfig = { GCPProjectID: '', SpannerInstanceID: '' }
  }

  ngOnInit(): void {
    this.data.config.subscribe((res: ISpannerConfig) => {
      this.spannerConfig = res
    })

    this.data.isOffline.subscribe({
      next: (res) => {
        this.isOfflineStatus = res
      },
    })
  }

  openEditForm() {
    let openDialog = this.dialog.open(UpdateSpannerConfigFormComponent, {
      maxWidth: '500px',
      data: this.spannerConfig,
    })
    openDialog.afterClosed().subscribe((data: ISpannerConfig) => {
      console.log(data)
      if (data) {
        this.spannerConfig = data
      }
    })
  }

  showWarning() {
    return !this.spannerConfig.GCPProjectID && !this.spannerConfig.SpannerInstanceID
  }
  openInstructionSidenav() {
    this.sidenav.openSidenav()
    this.sidenav.setSidenavComponent('instruction')
  }
}
