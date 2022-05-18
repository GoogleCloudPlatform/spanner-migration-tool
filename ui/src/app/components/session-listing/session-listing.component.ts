import { Component, OnInit } from '@angular/core'
import { Router } from '@angular/router'
import { DataService } from 'src/app/services/data/data.service'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import ISession from '../../model/session'
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service'
import { ClickEventService } from 'src/app/services/click-event/click-event.service'
import IConv from 'src/app/model/conv'

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

  downloadSessionFile(
    versionId: string,
    sessionName: string,
    databaseType: string,
    databaseName: string
  ) {
    this.fetch.getConvForAsession(versionId).subscribe((data: any) => {
      var a = document.createElement('a')
      a.href = URL.createObjectURL(data)
      a.download = `${sessionName}_${databaseType}_${databaseName}.json`
      a.click()
    })
  }

  resumeFromSessionFile(versionId: string) {
    this.data.resetStore()
    this.data.getSchemaConversionFromResumeSession(versionId)
    this.data.conv.subscribe((res: IConv) => {
      this.router.navigate(['/workspace'])
    })
  }
  openSpannerConfigDialog() {
    this.clickEvent.openSpannerConfig()
  }

  convertDateTime(val: string) {
    console.log(val)
    let datetime = new Date(val)
    val = datetime.toString()
    val = val.substring(val.indexOf(' ') + 1)
    val = val.substring(0, val.indexOf('('))
    return val
  }
}
