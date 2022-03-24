import { Component, OnInit } from '@angular/core'
import ISpannerConfig from '../../model/SpannerConfig'
import { MatDialog } from '@angular/material/dialog'
import { UpdateSpannerConfigFormComponent } from '../update-spanner-config-form/update-spanner-config-form.component'
import { DataService } from 'src/app/services/data/data.service'

@Component({
  selector: 'app-header',
  templateUrl: './header.component.html',
  styleUrls: ['./header.component.scss'],
})
export class HeaderComponent implements OnInit {
  spannerConfig: ISpannerConfig
  constructor(private data: DataService, private dialog: MatDialog) {
    this.spannerConfig = { GCPProjectID: '', SpannerInstanceID: '' }
  }

  ngOnInit(): void {
    this.data.config.subscribe((res: ISpannerConfig) => {
      console.log(res)
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

  shouldShowWarning() {
    return !(this.spannerConfig.GCPProjectID === '' || this.spannerConfig.SpannerInstanceID === '')
  }
}
