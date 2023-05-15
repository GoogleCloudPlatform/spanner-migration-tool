import { Component, Inject, OnInit } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MigrationDetails, Profile, TargetDetails } from 'src/app/app.constants';
import IConnectionProfile, { ICreateConnectionProfile, ICreateConnectionProfileV2, ISetUpConnectionProfile } from 'src/app/model/profile';
import { FetchService } from 'src/app/services/fetch/fetch.service';
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service';

@Component({
  selector: 'app-connection-profile-form',
  templateUrl: './connection-profile-form.component.html',
  styleUrls: ['./connection-profile-form.component.scss']
})
export class ConnectionProfileFormComponent implements OnInit {
  connectionProfileForm: FormGroup
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
  sourceDatabaseType: string = ''
  testSuccess: boolean = false
  constructor(
    private fetch: FetchService,
    private snack: SnackbarService,
    private formBuilder: FormBuilder,
    private dialogRef: MatDialogRef<ConnectionProfileFormComponent>,
    @Inject(MAT_DIALOG_DATA) public data: ISetUpConnectionProfile
  ) {
    this.isSource = data.IsSource
    this.sourceDatabaseType = data.SourceDatabaseType
    if (!this.isSource) {
      this.profileType = Profile.TargetProfileType
    }
    this.connectionProfileForm = this.formBuilder.group({
      profileOption: ['', Validators.required],
      newProfile: [],
      existingProfile: [],
      replicationSlot: [],
      publication: [],
    })
    if (this.sourceDatabaseType == 'postgres' && this.isSource) {
      this.connectionProfileForm.get('replicationSlot')?.addValidators([Validators.required])
      this.connectionProfileForm.controls['replicationSlot'].updateValueAndValidity()
      this.connectionProfileForm.get('publication')?.addValidators([Validators.required])
      this.connectionProfileForm.controls['publication'].updateValueAndValidity()
    }
    this.getConnectionProfilesAndIps()
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
      IsSource: this.isSource,
      ValidateOnly: true
    }
    this.fetch.createConnectionProfile(payload).subscribe({
      next: () => {
        this.testSuccess = true
      },
      error: (err: any) => {
        this.testSuccess = false
        this.errorMsg = err
      },
    })
  }
  createConnectionProfile() {
    let formValue = this.connectionProfileForm.value
    if (this.isSource) {
      localStorage.setItem(TargetDetails.ReplicationSlot, formValue.replicationSlot)
      localStorage.setItem(TargetDetails.Publication, formValue.publication)
    }
    if (this.selectedOption === Profile.NewConnProfile) {
      let payload: ICreateConnectionProfileV2 = {
        Id: formValue.newProfile,
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

  getConnectionProfilesAndIps() {
    this.fetch.getConnectionProfiles(this.isSource).subscribe({
      next: (res: IConnectionProfile[]) => {
        this.profileList = res
      },
      error: (err: any) => {
        this.snack.openSnackBar(err.error, 'Close')
      },
    })
    if (this.isSource) {
      this.fetch.getStaticIps().subscribe({
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
