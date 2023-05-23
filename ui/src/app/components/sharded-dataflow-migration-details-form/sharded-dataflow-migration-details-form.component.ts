import { Component, Inject, OnInit } from '@angular/core';
import { FormArray, FormBuilder, FormGroup, Validators } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MigrationDetails, Profile, StorageKeys } from 'src/app/app.constants';
import IDbConfig from 'src/app/model/db-config';
import IConnectionProfile, { ICreateConnectionProfile, ICreateConnectionProfileV2, IDataShard, IDatastreamConnProfile, IDirectConnectionConfig, ILogicalShard, IMigrationProfile, ISetUpConnectionProfile, IShardConfigurationDataflow, IShardedDataflowMigration } from 'src/app/model/profile';
import { FetchService } from 'src/app/services/fetch/fetch.service';
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service';

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
  selectedSourceProfileOption = Profile.NewConnProfile
  selectedTargetProfileOption = Profile.NewConnProfile
  profileOptions = [
    { value: Profile.NewConnProfile, display: 'Create a new connection profile' },
    { value: Profile.ExistingConnProfile, display: 'Choose an existing connection profile' },
  ]
  profileName = ''
  errorMsg = ''
  sourceDatabaseType: string = ''
  testSuccess: boolean = false
  createSrcConnSuccess: boolean = false
  createTgtConnSuccess: boolean = false
  region: string

  inputOptionsList = [
    { value: 'text', displayName: 'Text' },
    { value: 'form', displayName: 'Form' }
  ]

  schemaSourceConfig!: IDbConfig

  constructor(
    private fetch: FetchService,
    private snack: SnackbarService,
    private formBuilder: FormBuilder,
    private dialogRef: MatDialogRef<ShardedDataflowMigrationDetailsFormComponent>,
    @Inject(MAT_DIALOG_DATA) public data: IShardedDataflowMigration
  ) {
    this.region = data.Region
    this.sourceDatabaseType = data.SourceDatabaseType
    this.schemaSourceConfig = JSON.parse(localStorage.getItem(StorageKeys.Config) as string)
    let shardTableRowForm: FormGroup = this.formBuilder.group({
      logicalShardId: ['', Validators.required],
      dbName: ['', Validators.required]
    });
    this.migrationProfileForm = this.formBuilder.group({
      inputType: ['form', Validators.required],
      textInput: [],
      sourceProfileOption: [Profile.NewConnProfile, Validators.required],
      targetProfileOption: [Profile.NewConnProfile, Validators.required],
      newSourceProfile: [],
      existingSourceProfile: [],
      newTargetProfile: [],
      existingTargetProfile: [],
      host: [this.schemaSourceConfig.hostName, Validators.required],
      user: [this.schemaSourceConfig.userName, Validators.required],
      port: [this.schemaSourceConfig.port, Validators.required],
      password: [this.schemaSourceConfig.password, Validators.required],
      dataShardId: ['', Validators.required],
      shardMappingTable: this.formBuilder.array([shardTableRowForm])
    })

    let schemaSource: IDirectConnectionConfig = {
      host: this.schemaSourceConfig.hostName,
      user: this.schemaSourceConfig.userName,
      password: this.schemaSourceConfig.password,
      port: this.schemaSourceConfig.port,
      dbName: this.schemaSourceConfig.dbName
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
          this.sourceProfileList = res
        } else {
          this.targetProfileList = res
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
      } else {
        this.migrationProfileForm.controls['newSourceProfile'].clearValidators()
        this.migrationProfileForm.get('existingSourceProfile')?.addValidators([Validators.required])
        this.migrationProfileForm.controls['newSourceProfile'].updateValueAndValidity()
        this.migrationProfileForm.controls['existingSourceProfile'].updateValueAndValidity()
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

  saveDetailsAndReset() {
    this.handleConnConfigsFromForm()
    let shardTableRowForm: FormGroup = this.formBuilder.group({
      logicalShardId: ['', Validators.required],
      dbName: ['', Validators.required]
    });
    this.migrationProfileForm = this.formBuilder.group({
      inputType: ['form', Validators.required],
      textInput: [],
      sourceProfileOption: [Profile.NewConnProfile],
      targetProfileOption: [Profile.NewConnProfile],
      newSourceProfile: [],
      existingSourceProfile: [],
      newTargetProfile: [],
      existingTargetProfile: [],
      host: [],
      user: [],
      port: [],
      password: [],
      dataShardId: [],
      shardMappingTable: this.formBuilder.array([shardTableRowForm])
    })
  }

  finalizeConnDetails() {
    let formValue = this.migrationProfileForm.value
    let inputType: string = formValue.inputType
    if (inputType === "form") {
      //save the latest values from the form in the arrays as well
      this.handleConnConfigsFromForm()
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
    this.fetch.setShardSourceDBDetailsForDataflow(this.migrationProfile).subscribe({
      next: () => {
        localStorage.setItem(MigrationDetails.IsSourceConnectionProfileSet, "true")
        localStorage.setItem(MigrationDetails.IsTargetConnectionProfileSet, "true")
        this.dialogRef.close()
      },
      error: (err: any) => {
        this.errorMsg = err.error
      }
    })
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
  }

  createOrTestConnection(isSource: boolean, isValidateOnly: boolean) {
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
          this.testSuccess = true
        } else {
          if (isSource) {
            this.createSrcConnSuccess = true
          } else {
            this.createTgtConnSuccess = true
          }
        }
      },
      error: (err: any) => {
        if (isValidateOnly) {
          this.testSuccess = false
        } else {
          if (isSource) {
            this.createSrcConnSuccess = false
          } else {
            this.createTgtConnSuccess = false
          }
        }
        console.log(err)
        this.errorMsg = err
      },
    })
  }
}
