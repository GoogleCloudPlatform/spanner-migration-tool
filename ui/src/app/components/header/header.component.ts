import { Component, OnInit } from '@angular/core'
import ISpannerConfig from '../../model/spanner-config'
import { MatDialog } from '@angular/material/dialog'
import { UpdateSpannerConfigFormComponent } from '../update-spanner-config-form/update-spanner-config-form.component'
import { DataService } from 'src/app/services/data/data.service'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'
import { ClickEventService } from 'src/app/services/click-event/click-event.service'
import { LoaderService } from 'src/app/services/loader/loader.service'

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
    private sidenav: SidenavService,
    private clickEvent: ClickEventService,
    private loaderService: LoaderService
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
    this.clickEvent.spannerConfig.subscribe((res) => {
      if (res) {
        this.openEditForm()
      }
    })
  }

  openEditForm() {
    let openDialog = this.dialog.open(UpdateSpannerConfigFormComponent, {
      width: '30vw',
      minWidth: '400px',
      maxWidth: '500px',
      data: this.spannerConfig,
    })
    openDialog.afterClosed().subscribe((data: ISpannerConfig) => {
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
  openUserGuide() {
    window.open('https://github.com/GoogleCloudPlatform/spanner-migration-tool/blob/master/SpannerMigrationToolUIUserGuide.pdf', '_blank');
  }
  stopLoading() {
    this.loaderService.stopLoader()
    this.clickEvent.cancelDbLoading()
    this.clickEvent.closeDatabaseLoader()
  }
}
