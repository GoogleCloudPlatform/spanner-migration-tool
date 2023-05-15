import { Component, Inject, OnInit } from '@angular/core';
import { FormControl, FormGroup, Validators } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MigrationDetails, StorageKeys } from 'src/app/app.constants';
import IDbConfig, { IDbConfigs, IShardSessionDetails } from 'src/app/model/db-config';
import { DataService } from 'src/app/services/data/data.service';
import { FetchService } from 'src/app/services/fetch/fetch.service';

@Component({
  selector: 'app-sharded-bulk-source-details-form',
  templateUrl: './sharded-bulk-source-details-form.component.html',
  styleUrls: ['./sharded-bulk-source-details-form.component.scss']
})
export class ShardedBulkSourceDetailsFormComponent implements OnInit {

  errorMsg = ''
  shardConnDetailsList: Array<IDbConfig> = [];
  sourceConnDetails: IDbConfigs = {
    dbConfigs: [],
    isRestoredSession: ''
  };
  shardSessionDetails: IShardSessionDetails = {
    sourceDatabaseEngine: '',
    isRestoredSession: ''
  }

  directConnectForm!: FormGroup;

  inputOptionsList = [
    { value: 'text', displayName: 'Text' },
    { value: 'form', displayName: 'Form' }
  ]

  constructor(
    private fetch: FetchService,
    private dataService: DataService,
    private dialogRef: MatDialogRef<ShardedBulkSourceDetailsFormComponent>,
    @Inject(MAT_DIALOG_DATA) public data: IShardSessionDetails
  ) {
    this.shardSessionDetails = {
      sourceDatabaseEngine: data.sourceDatabaseEngine,
      isRestoredSession: data.isRestoredSession
    }
  }

  ngOnInit(): void {
    this.initFromLocalStorage()
  }

  initFromLocalStorage() {
    let schemaSourceConfig: IDbConfig = {
      dbEngine: '',
      isSharded: false,
      hostName: '',
      port: '',
      userName: '',
      password: '',
      dbName: ''
    }
    if (localStorage.getItem(StorageKeys.Config) != null) {
      schemaSourceConfig = JSON.parse(localStorage.getItem(StorageKeys.Config) as string)
    }
    this.directConnectForm = new FormGroup({
      inputType: new FormControl('form', [Validators.required]),
      textInput: new FormControl(''),
      hostName: new FormControl(schemaSourceConfig.hostName, [Validators.required]),
      port: new FormControl(schemaSourceConfig.port, [Validators.required]),
      userName: new FormControl(schemaSourceConfig.userName, [Validators.required]),
      dbName: new FormControl(schemaSourceConfig.dbName, [Validators.required]),
      password: new FormControl(schemaSourceConfig.password),
    })
  }

  saveDetailsAndReset() {
    const { hostName, port, userName, password, dbName } = this.directConnectForm.value
    let connConfig: IDbConfig = {
      dbEngine: this.shardSessionDetails.sourceDatabaseEngine,
      isSharded: false,
      hostName: hostName,
      port: port,
      userName: userName,
      password: password,
      dbName: dbName,
    }
    this.shardConnDetailsList.push(connConfig)
    this.directConnectForm.reset()
    this.directConnectForm = new FormGroup({
      inputType: new FormControl('form'),
      textInput: new FormControl(''),
      hostName: new FormControl(''),
      port: new FormControl(''),
      userName: new FormControl(''),
      dbName: new FormControl(''),
      password: new FormControl(''),
    })
  }
  finalizeConnDetails() {
    let inputType: string = this.directConnectForm.value.inputType
    if (inputType === "form") {
      const { hostName, port, userName, password, dbName } = this.directConnectForm.value
      let connConfig: IDbConfig = {
        dbEngine: this.shardSessionDetails.sourceDatabaseEngine,
        isSharded: false,
        hostName: hostName,
        port: port,
        userName: userName,
        password: password,
        dbName: dbName,
      }
      this.shardConnDetailsList.push(connConfig)
      this.sourceConnDetails.dbConfigs = this.shardConnDetailsList
    } else {
      try {
        this.sourceConnDetails.dbConfigs = JSON.parse(this.directConnectForm.value.textInput)
      } catch (err) {
        this.errorMsg = 'Unable to parse JSON'
        throw new Error(this.errorMsg);
      }
      
      this.sourceConnDetails.dbConfigs.forEach( (dbConfig) => {
        dbConfig.dbEngine = this.shardSessionDetails.sourceDatabaseEngine
      })
    }
    this.sourceConnDetails.isRestoredSession = this.shardSessionDetails.isRestoredSession
    this.fetch.setShardsSourceDBDetailsForBulk(this.sourceConnDetails).subscribe({
      next: () => {
        localStorage.setItem(MigrationDetails.IsSourceDetailsSet, "true")
        localStorage.setItem(MigrationDetails.NumberOfShards, this.sourceConnDetails.dbConfigs.length.toString())
        this.dialogRef.close()
      },
      error: (err: any) => {
        this.errorMsg = err.error
      }
    })
  }

}
