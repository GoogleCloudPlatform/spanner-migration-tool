import { Component, Inject, OnInit } from '@angular/core';
import { AbstractControl, FormArray, FormBuilder, FormGroup, Validators } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { InputType, MigrationDetails, Profile, StorageKeys } from 'src/app/app.constants';
import IDbConfig from 'src/app/model/db-config';
import IConnectionProfile, { ICreateConnectionProfileV2, IDataShard, IDatastreamConnProfile, IDirectConnectionConfig, ILogicalShard, IMigrationProfile, IShardConfigurationDataflow, IShardedDataflowMigration } from 'src/app/model/profile';
import { FetchService } from 'src/app/services/fetch/fetch.service';
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service';
import { DataService } from 'src/app/services/data/data.service'

@Component({
  selector: 'app-sharded-dataflow-migration-details-form',
  templateUrl: './sharded-dataflow-migration-details-form.component.html',
  styleUrls: ['./sharded-dataflow-migration-details-form.component.scss']
})
export class ShardedDataflowMigrationDetailsFormComponent implements OnInit {

  migrationProfileForm!: FormGroup
  selectedProfile: string = ''
  profileType: string = ''
  sourceProfileList: IConnectionProfile[] = []
  targetProfileList: IConnectionProfile[] = []
  definedSrcConnProfileList: IDatastreamConnProfile[] = []
  definedTgtConnProfileList: IDatastreamConnProfile[] = []
  shardIdToDBMappingTable: ILogicalShard[][] = []
  dataShardIdList: string[] = []
  migrationProfile: IMigrationProfile;
  ipList: string[] = []
  selectedSourceProfileOption = Profile.ExistingConnProfile
  selectedTargetProfileOption = Profile.ExistingConnProfile
  profileOptions = [
    { value: Profile.ExistingConnProfile, display: 'Choose an existing connection profile' },
    { value: Profile.NewConnProfile, display: 'Create a new connection profile' },
  ]
  profileName = ''
  errorMsg = ''
  errorSrcMsg = ''
  errorTgtMsg = ''
  errorVerMsg = ''
  sourceDatabaseType: string = ''
  inputValue: string = ''
  testSuccess: boolean = false
  verifyJson: boolean =  false
  createSrcConnSuccess: boolean = false
  createTgtConnSuccess: boolean = false
  region: string
  physicalShards: number = 0
  logicalShards: number = 0
  testingSourceConnection: boolean = false
  creatingSourceConnection: boolean = false
  creatingTargetConnection: boolean = false
  verifyingJson: boolean = false
  prefix: string = 'smt-datashard';

  inputOptionsList = [
    { value: 'text', displayName: 'Text' },
    { value: 'form', displayName: 'Form' }
  ]

  schemaSourceConfig!: IDbConfig

  constructor(
    private fetch: FetchService,
    private snack: SnackbarService,
    private dataService :DataService,
    private formBuilder: FormBuilder,
    private dialogRef: MatDialogRef<ShardedDataflowMigrationDetailsFormComponent>,
    @Inject(MAT_DIALOG_DATA) public data: IShardedDataflowMigration
  ) {
    this.region = data.Region
    this.sourceDatabaseType = data.SourceDatabaseType
    let inputType = localStorage.getItem(StorageKeys.Type) as string
    if (inputType == InputType.DirectConnect) {
      this.schemaSourceConfig = JSON.parse(localStorage.getItem(StorageKeys.Config) as string)
      console.log(this.schemaSourceConfig)
    }

    
    let shardTableRowForm: FormGroup = this.formBuilder.group({
      logicalShardId: ['', Validators.required],
      dbName: ['', Validators.required]
    });
    this.inputValue = this.prefix +"-"+this.randomString(4)+"-"+this.randomString(4);
    this.migrationProfileForm = this.formBuilder.group({
      inputType: ['form', Validators.required],
      textInput: [''],
      sourceProfileOption: [Profile.NewConnProfile, Validators.required],
      targetProfileOption: [Profile.NewConnProfile, Validators.required],
      newSourceProfile: ['',[Validators.pattern('^[a-z][a-z0-9-]{0,59}$')]],
      existingSourceProfile: [],
      newTargetProfile: ['',Validators.pattern('^[a-z][a-z0-9-]{0,59}$')],
      existingTargetProfile: [],
      host: [this.schemaSourceConfig?.hostName],
      user: [this.schemaSourceConfig?.userName],
      port: [this.schemaSourceConfig?.port],
      password: [this.schemaSourceConfig?.password],
      dataShardId: [this.inputValue,Validators.required],
      shardMappingTable: this.formBuilder.array([shardTableRowForm])
    })

    let schemaSource: IDirectConnectionConfig = {
      host: this.schemaSourceConfig?.hostName!,
      user: this.schemaSourceConfig?.userName!,
      password: this.schemaSourceConfig?.password!,
      port: this.schemaSourceConfig?.port!,
      dbName: this.schemaSourceConfig?.dbName!
    }
    let shardConfigurationDataflow: IShardConfigurationDataflow = {
      schemaSource: schemaSource,
      dataShards: []
    }
    let migrationProfile: IMigrationProfile = {
      configType: 'dataflow',
      shardConfigurationDataflow: shardConfigurationDataflow
    }
    this.migrationProfile = migrationProfile
  }

  ngOnInit(): void {
    this.getConnectionProfiles(true)
    this.getConnectionProfiles(false)
    this.getDatastreamIPs()
    this.initFromLocalStorage()
  }

  initFromLocalStorage() {

  }

  get shardMappingTable() {
    return this.migrationProfileForm.controls["shardMappingTable"] as FormArray;
  }

  addRow() {
    let shardTableRowForm: FormGroup = this.formBuilder.group({
      logicalShardId: ['', Validators.required],
      dbName: ['', Validators.required]
    });
    this.shardMappingTable.push(shardTableRowForm);
  }

  deleteRow(idx: number) {
    this.shardMappingTable.removeAt(idx);
  }

  getDatastreamIPs() {
    this.fetch.getStaticIps().subscribe({
      next: (res: string[]) => {
        this.ipList = res
      },
      error: (err: any) => {
        this.snack.openSnackBar(err.error, 'Close')
      },

    })
  }

  getConnectionProfiles(isSource: boolean) {
    this.fetch.getConnectionProfiles(isSource).subscribe({
      next: (res: IConnectionProfile[]) => {
        if (isSource) {
          this.sourceProfileList = res.sort((a,b) => a.DisplayName.localeCompare(b.DisplayName))
        } else {
          this.targetProfileList = res.sort((a,b) => a.DisplayName.localeCompare(b.DisplayName))
        }
      },
      error: (err: any) => {
        this.snack.openSnackBar(err.error, 'Close')
      },
    })
  }

  onItemChange(optionValue: string, profileType: string) {
    this.profileType = profileType
    if (this.profileType === 'source') {
      this.selectedSourceProfileOption = optionValue
      if (this.selectedSourceProfileOption == Profile.NewConnProfile) {
        this.migrationProfileForm.get('newSourceProfile')?.setValidators([Validators.required])
        this.migrationProfileForm.controls['existingSourceProfile'].clearValidators()
        this.migrationProfileForm.controls['newSourceProfile'].updateValueAndValidity()
        this.migrationProfileForm.controls['existingSourceProfile'].updateValueAndValidity()
        this.migrationProfileForm.get('host')?.setValidators([Validators.required])
        this.migrationProfileForm.controls['host'].updateValueAndValidity()
        this.migrationProfileForm.get('user')?.setValidators([Validators.required])
        this.migrationProfileForm.controls['user'].updateValueAndValidity()
        this.migrationProfileForm.get('port')?.setValidators([Validators.required])
        this.migrationProfileForm.controls['port'].updateValueAndValidity()
        this.migrationProfileForm.get('password')?.setValidators([Validators.required])
        this.migrationProfileForm.controls['password'].updateValueAndValidity()

      } else {
        this.migrationProfileForm.controls['newSourceProfile'].clearValidators()
        this.migrationProfileForm.get('existingSourceProfile')?.addValidators([Validators.required])
        this.migrationProfileForm.controls['newSourceProfile'].updateValueAndValidity()
        this.migrationProfileForm.controls['existingSourceProfile'].updateValueAndValidity()
        this.migrationProfileForm.controls['host'].clearValidators()
        this.migrationProfileForm.controls['host'].updateValueAndValidity()
        this.migrationProfileForm.controls['user'].clearValidators()
        this.migrationProfileForm.controls['user'].updateValueAndValidity()
        this.migrationProfileForm.controls['port'].clearValidators()
        this.migrationProfileForm.controls['port'].updateValueAndValidity()
        this.migrationProfileForm.controls['password'].clearValidators()
        this.migrationProfileForm.controls['password'].updateValueAndValidity()
      }
    }
    else {
      this.selectedTargetProfileOption = optionValue
      if (this.selectedTargetProfileOption == Profile.NewConnProfile) {
        this.migrationProfileForm.get('newTargetProfile')?.setValidators([Validators.required])
        this.migrationProfileForm.controls['existingTargetProfile'].clearValidators()
        this.migrationProfileForm.controls['newTargetProfile'].updateValueAndValidity()
        this.migrationProfileForm.controls['existingTargetProfile'].updateValueAndValidity()
      } else {
        this.migrationProfileForm.controls['newTargetProfile'].clearValidators()
        this.migrationProfileForm.get('existingTargetProfile')?.addValidators([Validators.required])
        this.migrationProfileForm.controls['newTargetProfile'].updateValueAndValidity()
        this.migrationProfileForm.controls['existingTargetProfile'].updateValueAndValidity()
      }
    }
  }

  setValidators(inputType: string) {
    if (inputType === "text") {
      for (const key in this.migrationProfileForm.controls) {
        this.migrationProfileForm.controls[key].clearValidators()
        this.migrationProfileForm.controls[key].updateValueAndValidity()
      }
      const shardMappingTableArray = this.migrationProfileForm.get('shardMappingTable') as FormArray;
      shardMappingTableArray.controls.forEach((control: AbstractControl) => {
        const group = control as FormGroup;
        const logicalShardIdControl = group.get('logicalShardId');
        const dbNameControl = group.get('dbName');

        logicalShardIdControl?.clearValidators();
        logicalShardIdControl?.updateValueAndValidity();
        dbNameControl?.clearValidators();
        dbNameControl?.updateValueAndValidity();
      });
      this.migrationProfileForm.controls['textInput'].setValidators([Validators.required])
      this.migrationProfileForm.controls['textInput'].updateValueAndValidity()
    }
    else {
      this.onItemChange('new', 'source')
      this.onItemChange('new', 'target')
      this.migrationProfileForm.controls['textInput'].clearValidators()
      this.migrationProfileForm.controls['textInput'].updateValueAndValidity()
    }
  }

  saveDetailsAndReset() {
    this.handleConnConfigsFromForm()
    let shardTableRowForm: FormGroup = this.formBuilder.group({
      logicalShardId: ['', Validators.required],
      dbName: ['', Validators.required]
    });
    this.inputValue = this.prefix +"-"+this.randomString(4)+"-"+this.randomString(4);
    this.migrationProfileForm = this.formBuilder.group({
      inputType: ['form', Validators.required],
      textInput: [],
      sourceProfileOption: [this.selectedSourceProfileOption],
      targetProfileOption: [this.selectedTargetProfileOption],
      newSourceProfile: ['',[Validators.pattern('^[a-z][a-z0-9-]{0,59}$')]],
      existingSourceProfile: [],
      newTargetProfile: ['',Validators.pattern('^[a-z][a-z0-9-]{0,59}$')],
      existingTargetProfile: [],
      host: [],
      user: [],
      port: [],
      password: [],
      dataShardId: [this.inputValue],
      shardMappingTable: this.formBuilder.array([shardTableRowForm])
    })
    this.testSuccess = false
    this.verifyJson = false
    this.createSrcConnSuccess = false
    this.createTgtConnSuccess = false
    this.snack.openSnackBar('Shard configured successfully, please configure the next', 'Close', 5)
  }

  randomString(length: number) {
    var randomChars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
    var result = '';
    for ( var i = 0; i < length; i++ ) {
        result += randomChars.charAt(Math.floor(Math.random() * randomChars.length));
    }
    return result;
}

  finalizeConnDetails() {
    let formValue = this.migrationProfileForm.value
    let inputType: string = formValue.inputType
    if (inputType === "form") {
      //The user can also hit "Finish" while trying to 
      //configure a non-first shard, in that case we should
      //consider it a valid shard only if the full information
      //is provided.
      if (this.migrationProfileForm.valid) {
        if ((this.selectedSourceProfileOption !== Profile.NewConnProfile || this.createSrcConnSuccess) &&
        (this.selectedTargetProfileOption !== Profile.NewConnProfile || this.createTgtConnSuccess)) {
          this.handleConnConfigsFromForm()
        }
      }
      //create the configuration to be passed to the backend.
      let dataShards: Array<IDataShard> = []
      //this can be the length of any of the lists
      const numShards = this.definedSrcConnProfileList.length;
      for (let i = 0; i < numShards; i++) {
        const dataShardId = this.dataShardIdList[i]
        const srcConnProfile = this.definedSrcConnProfileList[i]
        const tgtConnProfile = this.definedTgtConnProfileList[i]
        const shardIdToDBMapping = this.shardIdToDBMappingTable[i]
        let dataShard: IDataShard = {
          dataShardId: dataShardId,
          srcConnectionProfile: srcConnProfile,
          dstConnectionProfile: tgtConnProfile,
          streamLocation: this.region,
          databases: shardIdToDBMapping
        }
        dataShards.push(dataShard)
      }
      this.migrationProfile.shardConfigurationDataflow.dataShards = dataShards
    } else {
      try {
        this.migrationProfile = JSON.parse(formValue.textInput)
      } catch (err) {
        this.errorMsg = 'Unable to parse JSON'
        throw new Error(this.errorMsg)
      }
    }

    this.fetch.setShardsSourceDBDetailsForDataflow(this.migrationProfile).subscribe({
      next: () => {
        localStorage.setItem(MigrationDetails.IsSourceConnectionProfileSet, "true")
        localStorage.setItem(MigrationDetails.IsTargetConnectionProfileSet, "true")
        localStorage.setItem(MigrationDetails.NumberOfShards, this.determineTotalLogicalShardsConfigured().toString())
        localStorage.setItem(MigrationDetails.NumberOfInstances, this.migrationProfile.shardConfigurationDataflow.dataShards.length.toString())
        this.dialogRef.close()
      },
      error: (err: any) => {
        this.errorMsg = err.error
      }
    })
  }

  determineTotalLogicalShardsConfigured(): number {
    let totalLogicalShards: number = 0
    this.migrationProfile.shardConfigurationDataflow.dataShards.forEach(dataShard => {
      totalLogicalShards = totalLogicalShards + dataShard.databases.length
    })
    return totalLogicalShards
  }

  handleConnConfigsFromForm() {
    let formValue = this.migrationProfileForm.value
    //save each shard's dataShardId in array
    this.dataShardIdList.push(formValue.dataShardId)
    //save each shard's source connection profile information in array
    if (this.selectedSourceProfileOption === Profile.NewConnProfile) {
      let srcConnProfile: IDatastreamConnProfile = {
        name: formValue.newSourceProfile,
        location: this.region
      }
      this.definedSrcConnProfileList.push(srcConnProfile)
    } else {
      let srcConnProfile: IDatastreamConnProfile = {
        name: formValue.existingSourceProfile,
        location: this.region
      }
      this.definedSrcConnProfileList.push(srcConnProfile)
    }
    //save each shard's target connection profile information in array
    if (this.selectedTargetProfileOption === Profile.NewConnProfile) {
      let tgtConnProfile: IDatastreamConnProfile = {
        name: formValue.newTargetProfile,
        location: this.region
      }
      this.definedTgtConnProfileList.push(tgtConnProfile)
    } else {
      let tgtConnProfile: IDatastreamConnProfile = {
        name: formValue.existingTargetProfile,
        location: this.region
      }
      this.definedTgtConnProfileList.push(tgtConnProfile)
    }
    //save each shard's shardId to dbName mapping information in array
    let shardIdToDBMapping: ILogicalShard[] = []
    for (let control of this.shardMappingTable.controls) {
      if (control instanceof FormGroup) {
        const shardFromVal = control.value
        let logicalShard: ILogicalShard = {
          dbName: shardFromVal.dbName,
          databaseId: shardFromVal.logicalShardId,
          refDataShardId: formValue.dataShardId
        }
        shardIdToDBMapping.push(logicalShard)
      }
    }
    this.shardIdToDBMappingTable.push(shardIdToDBMapping)
    this.physicalShards++
    this.logicalShards = this.logicalShards + shardIdToDBMapping.length
  }

  determineFormValidity(): boolean {
    if (this.migrationProfileForm.valid && (this.selectedSourceProfileOption !== Profile.NewConnProfile || this.createSrcConnSuccess) &&
    (this.selectedTargetProfileOption !== Profile.NewConnProfile || this.createTgtConnSuccess) && (this.migrationProfileForm.value.inputType === "text" === this.verifyJson)) {
      return true
    }
    return false
  }

  determineFinishValidity(): boolean {
    if (this.definedSrcConnProfileList.length > 0) {
      //means atleast one shard is configured. Finish should be enabled in this case.
      //if all the values are filled, the last form values should be converted
      //into a shard and if the user decides midway to hit finish, the partially filled 
      //values should be discarded. This handling will be done in handleConnConfigsFromForm()
      //method
      return true
    }
    else {
      //this is the first shard being configured, and user wants to hit Finish
      //Enable the button if the form is valid so that the shard config can be submitted on button click.
      return this.determineFormValidity()
    }
  }

  determineTextJsonValidity(): boolean {
    return this.migrationProfileForm.valid
  }
 
  determineConnectionProfileInfoValidity(): boolean {
    let formValue = this.migrationProfileForm.value
    return formValue.host != null && formValue.port != null && formValue.user != null && formValue.password != null && formValue.newSourceProfile != null
  }

  verifyTextJson() {
    this.verifyingJson = true
    let formValue = this.migrationProfileForm.value
    this.dataService.verifyJsonCfg(formValue.textInput).subscribe()
    this.verifyJson = true
    this.verifyingJson = false
  }

  createOrTestConnection(isSource: boolean, isValidateOnly: boolean) {
    if (isValidateOnly) {
      this.testingSourceConnection = true
    } else {
      if (isSource) {
        this.creatingSourceConnection = true
      } else {
        this.creatingTargetConnection = true
      }
    }
    
    let formValue = this.migrationProfileForm.value
    let payload: ICreateConnectionProfileV2
    if (isSource) {
      payload = {
        Id: formValue.newSourceProfile,
        IsSource: true,
        ValidateOnly: isValidateOnly,
        Host: formValue.host,
        Port: formValue.port,
        User: formValue.user,
        Password: formValue.password
      }
    } else {
      payload = {
        Id: formValue.newTargetProfile,
        IsSource: false,
        ValidateOnly: isValidateOnly,
      }
    }
    this.fetch.createConnectionProfile(payload).subscribe({
      next: () => {
        if (isValidateOnly) {
          this.testingSourceConnection = false
          this.testSuccess = true
        } else {
          if (isSource) {
            this.createSrcConnSuccess = true
            this.errorSrcMsg = ''
            this.creatingSourceConnection = false
          } else {
            this.createTgtConnSuccess = true
            this.errorTgtMsg = ''
            this.creatingTargetConnection = false
          }
        }
      },
      error: (err: any) => {
        if (isValidateOnly) {
          this.testingSourceConnection = false
          this.testSuccess = false
          this.errorSrcMsg = err.error
        } else {
          if (isSource) {
            this.createSrcConnSuccess = false
            this.errorSrcMsg = err.error
            this.creatingSourceConnection = false
          } else {
            this.createTgtConnSuccess = false
            this.errorTgtMsg = err.error
            this.creatingTargetConnection = false
          }
        }

      },
    })
  }
}
