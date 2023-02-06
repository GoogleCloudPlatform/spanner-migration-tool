import { Component, Inject, OnInit } from '@angular/core';
import { FormGroup, FormControl, Validators } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { InputType, MigrationDetails } from 'src/app/app.constants';
import IDbConfig from 'src/app/model/db-config';
import IDumpConfig from 'src/app/model/dump-config';
import { DataService } from 'src/app/services/data/data.service';
import { FetchService } from 'src/app/services/fetch/fetch.service';
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service';

@Component({
  selector: 'app-source-details-form',
  templateUrl: './source-details-form.component.html',
  styleUrls: ['./source-details-form.component.scss']
})
export class SourceDetailsFormComponent implements OnInit {
  inputOptions = [
    { value: InputType.DumpFile, display: 'Connect via dump file' },
    { value: InputType.DirectConnect, display: 'Connect via direct connection' },
  ]
  selectedOption: string = InputType.DirectConnect
  sourceDatabaseEngine: string = ''
  errorMsg = ''
  fileToUpload: File | null = null

  uploadStart: boolean = false
  uploadSuccess: boolean = false
  uploadFail: boolean = false
  constructor(
    private fetch: FetchService,
    private dataService: DataService,
    private snack: SnackbarService,
    private dialogRef: MatDialogRef<SourceDetailsFormComponent>,
    @Inject(MAT_DIALOG_DATA) public data: string
  ) {
    this.sourceDatabaseEngine = data
  }
  dumpFileForm = new FormGroup({
    filePath: new FormControl('', [Validators.required]),
  })
  directConnectForm = new FormGroup({
    hostName: new FormControl('', [Validators.required]),
    port: new FormControl('', [Validators.required]),
    userName: new FormControl('', [Validators.required]),
    dbName: new FormControl('', [Validators.required]),
    password: new FormControl(''),
  })

  ngOnInit(): void {
  }
  setSourceDBDetailsForDump() {
    const { filePath } = this.dumpFileForm.value
    let payload: IDumpConfig = {
      Driver: this.sourceDatabaseEngine,
      Path: filePath,
    }
    this.fetch.setSourceDBDetailsForDump(payload).subscribe({
      next: () => {
        localStorage.setItem(MigrationDetails.IsSourceDetailsSet, "true")
        this.dialogRef.close()
      },
      error: (err: any) => {
        this.errorMsg = err.error
        console.log(this.errorMsg)
      }
    })
  }

  setSourceDBDetailsForDirectConnect() {
    const { hostName, port, userName, password, dbName } = this.directConnectForm.value
    let payload: IDbConfig = {
      dbEngine: this.sourceDatabaseEngine,
      hostName: hostName,
      port: port,
      userName: userName,
      password: password,
      dbName: dbName,
    }
    this.fetch.setSourceDBDetailsForDirectConnect(payload).subscribe({
      next: () => {
        localStorage.setItem(MigrationDetails.IsSourceDetailsSet, "true")
        this.dialogRef.close()
      },
      error: (err: any) => {
        this.errorMsg = err.error
      }
    })

  }

  handleFileInput(e: Event) {
    let files: FileList | null = (e.target as HTMLInputElement).files
    if (files) {
      this.fileToUpload = files.item(0)
      this.dumpFileForm.patchValue({ filePath: this.fileToUpload?.name })
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
      this.dataService.uploadFile(uploadFormData).subscribe((res: string) => {
        if (res == '') {
          this.uploadSuccess = true
        } else {
          this.uploadFail = true
        }
      })
    }
  }
  onItemChange(optionValue: string) {
    this.selectedOption = optionValue
    this.errorMsg = ''
  }
}
