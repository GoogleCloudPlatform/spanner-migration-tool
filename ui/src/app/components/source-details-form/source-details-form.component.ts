import { Component, Inject, OnInit } from '@angular/core';
import { FormGroup, FormControl, Validators } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { InputType, MigrationDetails } from 'src/app/app.constants';
import IDbConfig from 'src/app/model/db-config';
import IDumpConfig from 'src/app/model/dump-config';
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
  constructor(
    private fetch: FetchService,
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

  onItemChange(optionValue: string) {
    this.selectedOption = optionValue
    this.errorMsg = ''
  }
}
