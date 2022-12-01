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
    { value: 'mysql', displayName: 'MYSQL' },
    { value: 'sqlserver', displayName: 'SQL Server' },
    { value: 'oracle', displayName: 'ORACLE' },
    { value: 'postgres', displayName: 'PostgreSQL' },
  ]
  fileToUpload: File | null = null
  uploadStart: boolean = false
  uploadSuccess: boolean = false
  uploadFail: boolean = false

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
  handleFileInput(e: Event) {
    let files: FileList | null = (e.target as HTMLInputElement).files
    if (files) {
      this.fileToUpload = files.item(0)
      this.connectForm.patchValue({ filePath: this.fileToUpload?.name })
      if (this.fileToUpload) {
        this.uploadFile()
      }
    }
  }
  uploadFile() {
    if (this.fileToUpload) {
      this.uploadStart = true
      this.uploadFail = false
      this.uploadSuccess = false
      const uploadFormData = new FormData()
      uploadFormData.append('myFile', this.fileToUpload, this.fileToUpload?.name)
      this.data.uploadFile(uploadFormData).subscribe((res: string) => {
        if (res == '') {
          this.uploadSuccess = true
        } else {
          this.uploadFail = true
        }
      })
    }
  }
}
