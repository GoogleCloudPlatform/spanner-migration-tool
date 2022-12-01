import { Component, OnInit } from '@angular/core'
import { FormControl, FormGroup, Validators } from '@angular/forms'
import { DataService } from 'src/app/services/data/data.service'
import ISessionConfig from '../../model/session-config'
import { Router } from '@angular/router'
import { InputType, StorageKeys } from 'src/app/app.constants'
import { extractSourceDbName } from 'src/app/utils/utils'
import { ClickEventService } from 'src/app/services/click-event/click-event.service'

@Component({
  selector: 'app-load-session',
  templateUrl: './load-session.component.html',
  styleUrls: ['./load-session.component.scss'],
})
export class LoadSessionComponent implements OnInit {
  constructor(
    private data: DataService,
    private router: Router,
    private clickEvent: ClickEventService
  ) {}

  connectForm = new FormGroup({
    dbEngine: new FormControl('sqlserver', [Validators.required]),
    filePath: new FormControl('', [Validators.required]),
  })

  dbEngineList = [
    { value: 'mysql', displayName: 'MySQL' },
    { value: 'sqlserver', displayName: 'SQL Server' },
    { value: 'oracle', displayName: 'Oracle' },
    { value: 'postgres', displayName: 'PostgreSQL' },
  ]

  ngOnInit(): void {}

  convertFromSessionFile() {
    this.clickEvent.openDatabaseLoader('session', '')
    this.data.resetStore()
    const { dbEngine, filePath } = this.connectForm.value
    const payload: ISessionConfig = {
      driver: dbEngine,
      filePath: filePath,
    }
    this.data.getSchemaConversionFromSession(payload)
    this.data.conv.subscribe((res) => {
      localStorage.setItem(StorageKeys.Config, JSON.stringify(payload))
      localStorage.setItem(StorageKeys.Type, InputType.SessionFile)
      localStorage.setItem(StorageKeys.SourceDbName, extractSourceDbName(dbEngine))
      this.clickEvent.closeDatabaseLoader()
      this.router.navigate(['/workspace'])
    })
  }
}
