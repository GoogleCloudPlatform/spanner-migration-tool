import { Component, Inject, OnInit } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MigrationDetails, TargetDetails } from 'src/app/app.constants';
import IConnectionProfile, { ICreateConnectionProfile } from 'src/app/model/profile';
import { FetchService } from 'src/app/services/fetch/fetch.service';
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service';

@Component({
  selector: 'app-connection-profile-form',
  templateUrl: './connection-profile-form.component.html',
  styleUrls: ['./connection-profile-form.component.scss']
})
export class ConnectionProfileFormComponent implements OnInit {
  connectionProfileForm: FormGroup
  selectedRegion: string = localStorage.getItem(TargetDetails.Region) as string
  selectedProfile: string = ''
  profileType: string = "Source"
  profileList: IConnectionProfile[] = []
  ipList: string[] = []
  selectedOption = "new"
  profileOptions = [
    { value: 'new', display: 'Create a new connection profile' },
    { value: 'existing', display: 'Choose an existing connection profile' },
  ]
  profileName = ''
  isSource: boolean = false
  testSuccess: boolean = false
  errorMessage: string = ''
  constructor(
    private fetch: FetchService,
    private snack: SnackbarService,
    private formBuilder: FormBuilder,
    private dialogRef: MatDialogRef<ConnectionProfileFormComponent>,
    @Inject(MAT_DIALOG_DATA) public data: boolean
  ) {
    this.isSource = data
    if (!this.isSource) {
      this.profileType = "Target"
    }
    this.connectionProfileForm = this.formBuilder.group({
      profileOption: ['', Validators.required],
      newProfile: [],
      existingProfile: [],
      bucket: [],
    })
    if (this.selectedRegion != '') {
      this.getConnectionProfilesAndIps(localStorage.getItem(TargetDetails.Region) as string)
    }
  }

  onItemChange(optionValue: string) {
    this.selectedOption = optionValue
    if (this.selectedOption === 'new') {
      this.connectionProfileForm.get('newProfile')?.setValidators([Validators.required])
      this.connectionProfileForm.controls['existingProfile'].clearValidators()
      this.connectionProfileForm.get('bucket')?.setValidators([Validators.required])
      this.connectionProfileForm.controls['bucket'].updateValueAndValidity()
      this.connectionProfileForm.controls['newProfile'].updateValueAndValidity()
      this.connectionProfileForm.controls['existingProfile'].updateValueAndValidity()
    } else {
      this.connectionProfileForm.controls['newProfile'].clearValidators()
      this.connectionProfileForm.get('existingProfile')?.addValidators([Validators.required])
      this.connectionProfileForm.get('bucket')?.clearValidators()
      this.connectionProfileForm.controls['bucket'].updateValueAndValidity()
      this.connectionProfileForm.controls['newProfile'].updateValueAndValidity()
      this.connectionProfileForm.controls['existingProfile'].updateValueAndValidity()
    }
  }
  testConnection() {
    let formValue = this.connectionProfileForm.value
    let payload: ICreateConnectionProfile = {
      Id: formValue.newProfile,
      Region: this.selectedRegion,
      IsSource: this.isSource,
      ValidateOnly: true,
      Bucket: formValue.bucket
    }
    this.fetch.createConnectionProfile(payload).subscribe({
      next: () => {
        this.testSuccess = true
      },
      error: (err: any) => {
        console.log(err)
        this.errorMessage = err.error
      },
    })
  }
  createConnectionProfile() {
    let formValue = this.connectionProfileForm.value
    if (this.selectedOption === 'new') {
      let payload: ICreateConnectionProfile = {
        Id: formValue.newProfile,
        Region: this.selectedRegion,
        IsSource: this.isSource,
        ValidateOnly: false,
        Bucket: formValue.bucket
      }
      this.fetch.createConnectionProfile(payload).subscribe({
        next: () => {
          if (this.isSource) {
            localStorage.setItem(MigrationDetails.IsSourceConnectionProfileSet, "true")
            localStorage.setItem(TargetDetails.SourceConnProfile, formValue.newProfile)
          } else {
            localStorage.setItem(MigrationDetails.IsTargetConnectionProfileSet, "true")
            localStorage.setItem(TargetDetails.TargetConnProfile, formValue.newProfile)
          }
        },
        error: (err: any) => {
          this.snack.openSnackBar(err.error, 'Close')
        },
      })
    } else {
      if (this.isSource) {
        localStorage.setItem(MigrationDetails.IsSourceConnectionProfileSet, "true")
        localStorage.setItem(TargetDetails.SourceConnProfile, formValue.existingProfile)
      } else {
        localStorage.setItem(MigrationDetails.IsTargetConnectionProfileSet, "true")
        localStorage.setItem(TargetDetails.TargetConnProfile, formValue.existingProfile)
      }
    }
    
    this.dialogRef.close()
  }
  ngOnInit(): void { }

  getConnectionProfilesAndIps(selectedRegion: string) {
    this.fetch.getConnectionProfiles(selectedRegion, this.isSource).subscribe({
      next: (res: IConnectionProfile[]) => {
        this.profileList = res
      },
      error: (err: any) => {
        this.snack.openSnackBar(err.error, 'Close')
      },
    })
    if (this.isSource) {
      this.fetch.getStaticIps(selectedRegion).subscribe({
        next: (res: string[]) => {
          this.ipList = res
        },
        error: (err: any) => {
          this.snack.openSnackBar(err.error, 'Close')
        },

      })
    }
  }



}
