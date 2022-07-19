import { Component, OnInit } from '@angular/core'
import { Router } from '@angular/router'
import { DataService } from 'src/app/services/data/data.service'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import ISession from '../../model/session'
import { ClickEventService } from 'src/app/services/click-event/click-event.service'
import IConv from 'src/app/model/conv'
import { InputType, StorageKeys } from 'src/app/app.constants'

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
  filteredDataSource: any = []
  filterColumnsValue: any = {
    sessionName: '',
    editorName: '',
    databaseType: '',
    databaseName: '',
  }
  displayFilter: any = {
    sessionName: false,
    editorName: false,
    databaseType: false,
    databaseName: false,
  }
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
          this.filteredDataSource = sessions
          this.dataSource = sessions
        } else {
          this.filteredDataSource = []
          this.dataSource = []
        }
      },
    })
  }
  toggleFilterDisplay(key: string) {
    this.displayFilter[key] = !this.displayFilter[key]
  }
  updateFilterValue(event: Event, key: string) {
    event.stopPropagation()
    const filterValue = (event.target as HTMLInputElement).value
    this.filterColumnsValue[key] = filterValue
    this.applyFilter()
  }
  applyFilter() {
    this.filteredDataSource = this.dataSource
      .filter((data: any) => {
        if (
          data.SessionName.toLowerCase().includes(this.filterColumnsValue.sessionName.toLowerCase())
        )
          return true
        else return false
      })
      .filter((data: any) => {
        if (
          data.EditorName.toLowerCase().includes(this.filterColumnsValue.editorName.toLowerCase())
        )
          return true
        else return false
      })
      .filter((data: any) => {
        if (
          data.DatabaseType.toLowerCase().includes(
            this.filterColumnsValue.databaseType.toLowerCase()
          )
        )
          return true
        else return false
      })
      .filter((data: any) => {
        if (
          data.DatabaseName.toLowerCase().includes(
            this.filterColumnsValue.databaseName.toLowerCase()
          )
        )
          return true
        else return false
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
      localStorage.setItem(StorageKeys.Config, versionId)
      localStorage.setItem(StorageKeys.Type, InputType.ResumeSession)
      this.router.navigate(['/workspace'])
    })
  }
  openSpannerConfigDialog() {
    this.clickEvent.openSpannerConfig()
  }

  convertDateTime(val: string) {
    let datetime = new Date(val)
    val = datetime.toString()
    val = val.substring(val.indexOf(' ') + 1)
    val = val.substring(0, val.indexOf('('))
    return val
  }
}
