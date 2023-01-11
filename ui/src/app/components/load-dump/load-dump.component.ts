import { Component, OnInit } from '@angular/core'
import { FormControl, FormGroup, Validators } from '@angular/forms'
import IDumpConfig from 'src/app/model/dump-config'
import { DataService } from 'src/app/services/data/data.service'
import { Router } from '@angular/router'
import { InputType, StorageKeys } from 'src/app/app.constants'
import { extractSourceDbName } from 'src/app/utils/utils'
import { ClickEventService } from 'src/app/services/click-event/click-event.service'

@Component({
  selector: 'app-load-dump',
  templateUrl: './load-dump.component.html',
  styleUrls: ['./load-dump.component.scss'],
})
export class LoadDumpComponent implements OnInit {
  constructor(
    private data: DataService,
    private router: Router,
    private clickEvent: ClickEventService
  ) {}
  connectForm = new FormGroup({
    dbEngine: new FormControl('mysqldump', [Validators.required]),
    filePath: new FormControl('', [Validators.required]),
  })
  dbEngineList = [
    { value: 'mysqldump', displayName: 'MySQL' },
    { value: 'pg_dump', displayName: 'PostgreSQL' },
  ]
  fileToUpload: File | null = null

  uploadStart: boolean = false
  uploadSuccess: boolean = false
  uploadFail: boolean = false

  getSchemaRequest: any = null

  ngOnInit(): void {
    this.clickEvent.cancelDbLoad.subscribe({
      next: (res: boolean) => {
        if (res && this.getSchemaRequest) {
          this.getSchemaRequest.unsubscribe()
        }
      },
    })
  }

  convertFromDump() {
    this.clickEvent.openDatabaseLoader('dump', '')
    this.data.resetStore()
    localStorage.clear()
    const { dbEngine, filePath } = this.connectForm.value
    const payload: IDumpConfig = {
      Driver: dbEngine,
      Path: filePath,
    }
    this.getSchemaRequest = this.data.getSchemaConversionFromDump(payload)
    this.data.conv.subscribe((res) => {
      localStorage.setItem(StorageKeys.Config, JSON.stringify(payload))
      localStorage.setItem(StorageKeys.Type, InputType.DumpFile)
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
