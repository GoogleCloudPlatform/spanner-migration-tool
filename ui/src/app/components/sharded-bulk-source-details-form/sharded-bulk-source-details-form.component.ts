import { Component, Inject, OnInit } from '@angular/core';
import { FormControl, FormGroup, Validators } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { InputType, MigrationDetails, StorageKeys } from 'src/app/app.constants';
import IDbConfig, { IDbConfigs, IShardSessionDetails } from 'src/app/model/db-config';
import { DataService } from 'src/app/services/data/data.service';
import { FetchService } from 'src/app/services/fetch/fetch.service';
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service';

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
    private snack: SnackbarService,
    private dataService: DataService,
    private dialogRef: MatDialogRef<ShardedBulkSourceDetailsFormComponent>,
    @Inject(MAT_DIALOG_DATA) public data: IShardSessionDetails
  ) {
    this.shardSessionDetails = {
      sourceDatabaseEngine: data.sourceDatabaseEngine,
      isRestoredSession: data.isRestoredSession
    }

    let schemaSourceConfig: IDbConfig = {
      dbEngine: '',
      isSharded: false,
      hostName: '',
      port: '',
      userName: '',
      password: '',
      dbName: ''
    }
    let inputType = localStorage.getItem(StorageKeys.Type) as string
    if (inputType == InputType.DirectConnect && localStorage.getItem(StorageKeys.Config) != null) {
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
      shardId: new FormControl('', [Validators.required]),
    })
  }

  ngOnInit(): void {
    this.initFromLocalStorage()
  }

  initFromLocalStorage() {
  }

  setValidators(inputType: string) {
    if (inputType == "text") {
      for (const key in this.directConnectForm.controls) {
        this.directConnectForm.get(key)?.clearValidators();
        this.directConnectForm.get(key)?.updateValueAndValidity();
      }
      this.directConnectForm.get('textInput')?.setValidators([Validators.required])
      this.directConnectForm.controls['textInput'].updateValueAndValidity()
    }
    else {
      this.directConnectForm.get('hostName')?.setValidators([Validators.required])
      this.directConnectForm.controls['hostName'].updateValueAndValidity()
      this.directConnectForm.get('port')?.setValidators([Validators.required])
      this.directConnectForm.controls['port'].updateValueAndValidity()
      this.directConnectForm.get('userName')?.setValidators([Validators.required])
      this.directConnectForm.controls['userName'].updateValueAndValidity()
      this.directConnectForm.get('dbName')?.setValidators([Validators.required])
      this.directConnectForm.controls['dbName'].updateValueAndValidity()
      this.directConnectForm.get('password')?.setValidators([Validators.required])
      this.directConnectForm.controls['password'].updateValueAndValidity()
      this.directConnectForm.get('shardId')?.setValidators([Validators.required])
      this.directConnectForm.controls['shardId'].updateValueAndValidity()
      this.directConnectForm.controls['textInput'].clearValidators()
      this.directConnectForm.controls['textInput'].updateValueAndValidity()
    }
  }

  determineFormValidity(): boolean {
    if (this.shardConnDetailsList.length > 0) {
      //means atleast one shard is configured. Finish should be enabled in this case.
      //if all the values are filled, the last form values should be converted
      //into a shard and if the user decides midway to hit finish, the partially filled 
      //values should be discarded. This handling will be done in handleConnConfigsFromForm()
      //method
      return true
    }
    else if (this.directConnectForm.valid) {
      //this is the first shard being configured, and user wants to hit Finish
      //Enable the button so that the shard config can be submitted on button click.
      return true
    }
    else {
      //all other cases
      return false
    }
  }


  saveDetailsAndReset() {
    const { hostName, port, userName, password, dbName, shardId } = this.directConnectForm.value
    let connConfig: IDbConfig = {
      dbEngine: this.shardSessionDetails.sourceDatabaseEngine,
      isSharded: false,
      hostName: hostName,
      port: port,
      userName: userName,
      password: password,
      dbName: dbName,
      shardId: shardId,
    }
    this.shardConnDetailsList.push(connConfig)
    this.directConnectForm = new FormGroup({
      inputType: new FormControl('form', Validators.required),
      textInput: new FormControl(''),
      hostName: new FormControl(''),
      port: new FormControl(''),
      userName: new FormControl(''),
      dbName: new FormControl(''),
      password: new FormControl(''),
      shardId: new FormControl(''),
    })
    this.setValidators('form')
    this.snack.openSnackBar('Shard configured successfully, please configure the next', 'Close', 5)
  }
  finalizeConnDetails() {
    let inputType: string = this.directConnectForm.value.inputType
    if (inputType === "form") {
      const { hostName, port, userName, password, dbName, shardId } = this.directConnectForm.value
      let connConfig: IDbConfig = {
        dbEngine: this.shardSessionDetails.sourceDatabaseEngine,
        isSharded: false,
        hostName: hostName,
        port: port,
        userName: userName,
        password: password,
        dbName: dbName,
        shardId: shardId,
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
        localStorage.setItem(MigrationDetails.NumberOfShards, this.sourceConnDetails.dbConfigs.length.toString())
        localStorage.setItem(MigrationDetails.NumberOfInstances, this.sourceConnDetails.dbConfigs.length.toString())
        localStorage.setItem(MigrationDetails.IsSourceDetailsSet, "true")
        this.dialogRef.close()
      },
      error: (err: any) => {
        this.errorMsg = err.error
      }
    })
  }

}
