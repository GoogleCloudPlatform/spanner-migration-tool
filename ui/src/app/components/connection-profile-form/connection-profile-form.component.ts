import { Component, Inject, OnInit } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MigrationDetails, Profile, TargetDetails } from 'src/app/app.constants';
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
  profileType: string = Profile.SourceProfileType
  profileList: IConnectionProfile[] = []
  ipList: string[] = []
  selectedOption = Profile.NewConnProfile
  profileOptions = [
    { value: Profile.NewConnProfile, display: 'Create a new connection profile' },
    { value: Profile.ExistingConnProfile, display: 'Choose an existing connection profile' },
  ]
  profileName = ''
  errorMsg = ''
  isSource: boolean = false
  testSuccess: boolean = false
  constructor(
    private fetch: FetchService,
    private snack: SnackbarService,
    private formBuilder: FormBuilder,
    private dialogRef: MatDialogRef<ConnectionProfileFormComponent>,
    @Inject(MAT_DIALOG_DATA) public data: boolean
  ) {
    this.isSource = data
    if (!this.isSource) {
      this.profileType = Profile.TargetProfileType
    }
    this.connectionProfileForm = this.formBuilder.group({
      profileOption: ['', Validators.required],
      newProfile: [],
      existingProfile: [],
    })
    if (this.selectedRegion != '') {
      this.getConnectionProfilesAndIps(localStorage.getItem(TargetDetails.Region) as string)
    }
  }

  onItemChange(optionValue: string) {
    this.selectedOption = optionValue
    if (this.selectedOption == Profile.NewConnProfile) {
      this.connectionProfileForm.get('newProfile')?.setValidators([Validators.required])
      this.connectionProfileForm.controls['existingProfile'].clearValidators()
      this.connectionProfileForm.controls['newProfile'].updateValueAndValidity()
      this.connectionProfileForm.controls['existingProfile'].updateValueAndValidity()
    } else {
      this.connectionProfileForm.controls['newProfile'].clearValidators()
      this.connectionProfileForm.get('existingProfile')?.addValidators([Validators.required])
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
      ValidateOnly: true
    }
    this.fetch.createConnectionProfile(payload).subscribe({
      next: () => {
        this.testSuccess = true
      },
      error: (err: any) => {
        this.testSuccess = false
        console.log(err)
        this.errorMsg = err
      },
    })
  }
  createConnectionProfile() {
    let formValue = this.connectionProfileForm.value
    if (this.selectedOption === Profile.NewConnProfile) {
      let payload: ICreateConnectionProfile = {
        Id: formValue.newProfile,
        Region: this.selectedRegion,
        IsSource: this.isSource,
        ValidateOnly: false
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
          this.dialogRef.close()
        },
        error: (err: any) => {
          this.snack.openSnackBar(err.error, 'Close')
          this.dialogRef.close()
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
      this.dialogRef.close()
    }
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
