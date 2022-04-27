import { Component, Input, OnInit, SimpleChanges } from '@angular/core'
import { Router } from '@angular/router'
import { DataService } from 'src/app/services/data/data.service'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import ISession from '../../model/Session'
import ISpannerConfig from '../../model/SpannerConfig'
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service'
import { ClickEventService } from 'src/app/services/click-event/click-event.service'

@Component({
  selector: 'app-session-listing',
  templateUrl: './session-listing.component.html',
  styleUrls: ['./session-listing.component.scss'],
})
export class SessionListingComponent implements OnInit {
  displayedColumns = [
    'SessionName',
    'EditorName',
    'DatabaseType',
    'DatabaseName',
    'Notes',
    'CreateTimestamp',
    'Action',
  ]
  notesToggle: boolean[] = []

  dataSource: any = []
  constructor(
    private fetch: FetchService,
    private data: DataService,
    private router: Router,
    private snackbar: SnackbarService,
    private clickEvent: ClickEventService
  ) {}

  ngOnInit(): void {
    this.data.getAllSessions()
    this.data.sessions.subscribe({
      next: (sessions: ISession[]) => {
        if (sessions != null) {
          this.dataSource = sessions
        } else {
          this.dataSource = []
        }
      },
    })
  }

  downloadSessionFile(versionId: string) {
    this.fetch.getConvForAsession(versionId).subscribe((data: any) => {
      var a = document.createElement('a')
      a.href = URL.createObjectURL(data)
      a.download = versionId + '.session.json'
      a.click()
    })
  }

  ResumeFromSessionFile(versionId: string) {
    this.data.resetStore()
    this.data.getSchemaConversionFromResumeSession(versionId)
    this.data.conv.subscribe((res) => {
      this.router.navigate(['/workspace'])
    })
  }
  openSpannerConfigDialog() {
    this.clickEvent.openSpannerConfig()
  }
}
