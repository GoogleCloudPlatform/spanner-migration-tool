import { Component, Input, OnInit, SimpleChanges } from '@angular/core'
import { Router } from '@angular/router'
import { InputType, StorageKeys } from 'src/app/app.constants'
import { DataService } from 'src/app/services/data/data.service'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import ISession from '../../model/Session'

@Component({
  selector: 'app-session-listing',
  templateUrl: './session-listing.component.html',
  styleUrls: ['./session-listing.component.scss'],
})
export class SessionListingComponent implements OnInit {
  @Input() sessions: ISession[] = []

  displayedColumns = [
    'SessionName',
    'EditorName',
    'DatabaseType',
    'DatabaseName',
    'Notes',
    'CreatedOn',
    'Action',
  ]

  dataSource = this.sessions
  constructor(private fetch: FetchService, private data: DataService, private router: Router) {}

  ngOnInit(): void {}

  ngOnChanges(changes: SimpleChanges): void {
    let newSessions = changes?.['sessions'].currentValue
    this.dataSource = newSessions
    console.log(this.dataSource)
  }

  downloadSessionFile(versionId: string) {
    this.fetch.getConvForAsession(versionId).subscribe((data: any) => {
      console.log(data)
      var a = document.createElement('a')
      a.href = URL.createObjectURL(data)
      a.download = versionId + '.session.json'
      a.click()
    })
  }

  ResumeFromSessionFile(versionId: string) {
    this.data.resetStore()
    // const { dbEngine, filePath } = this.connectForm.value
    // const payload: ISessionConfig = {
    //   driver: dbEngine,
    //   filePath: filePath,
    // }
    // this.data.getSchemaConversionFromSession(payload)
    this.data.conv.subscribe((res) => {
      console.log(res)
      // localStorage.setItem(StorageKeys.Config, JSON.stringify(payload))
      // localStorage.setItem(StorageKeys.Type, InputType.SessionFile)
      this.router.navigate(['/workspace'])
    })
  }
}
