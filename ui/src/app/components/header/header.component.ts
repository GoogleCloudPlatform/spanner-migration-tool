import { Component, OnInit } from '@angular/core'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import ISpannerConfig from '../../model/SpannerConfig'
import { MatDialog } from '@angular/material/dialog'
import { UpdateSpannerConfigFormComponent } from '../update-spanner-config-form/update-spanner-config-form.component'

@Component({
  selector: 'app-header',
  templateUrl: './header.component.html',
  styleUrls: ['./header.component.scss'],
})
export class HeaderComponent implements OnInit {
  spannerConfig: ISpannerConfig
  constructor(private fetch: FetchService, private dialog: MatDialog) {
    this.spannerConfig = { GCPProjectID: '', SpannerInstanceID: '' }
  }

  ngOnInit(): void {
    this.fetch.getSpannerConfig().subscribe((res: ISpannerConfig) => {
      this.spannerConfig = res
    })
  }

  openEditForm() {
    let openDialog = this.dialog.open(UpdateSpannerConfigFormComponent, {
      maxWidth: '500px',
      data: this.spannerConfig,
    })
    openDialog.afterClosed().subscribe((data: ISpannerConfig) => {
      this.spannerConfig = data
    })
  }
}
