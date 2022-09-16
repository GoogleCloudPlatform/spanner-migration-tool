import { Component, Inject, OnInit } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { MAT_DIALOG_DATA } from '@angular/material/dialog';
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
  regionList = ['us-central1']
  selectedRegion: string = 'us-central1'
  selectedProfile: string = ''
  profileType: string = "Source"
  isRegionSelected: boolean = false
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
    @Inject(MAT_DIALOG_DATA) public data: boolean
  ) {
    this.isSource = data
    if (!this.isSource) {
      this.profileType = "Target"
    }
    this.connectionProfileForm = this.formBuilder.group({
      region: ['', Validators.required],
      profileOption: ['', Validators.required],
      newProfile: [],
      existingProfile: [],
      bucket: [],
    })
  }

  onItemChange(optionValue: string) {
    this.selectedOption = optionValue
    if (this.selectedOption === 'new') {
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
      Region: formValue.existingProfile,
      IsSource: this.isSource,
      ValidateOnly: true,
      Bucket: formValue.bucket
    }
    this.fetch.createConnectionProfile(payload).subscribe({
      next: () => {
        this.testSuccess = true
      },
      error: (err: any) => {
        this.errorMessage = err.error
      },
    })
  }
  createConnectionProfile() {
    let formValue = this.connectionProfileForm.value
    let payload: ICreateConnectionProfile = {
      Id: formValue.newProfile,
      Region: formValue.existingProfile,
      IsSource: this.isSource,
      ValidateOnly: false,
      Bucket: formValue.bucket
    }
    this.fetch.createConnectionProfile(payload).subscribe({
      next: () => {
        localStorage.setItem(this.profileType+"ProfileSet","true")
      },
      error: (err: any) => {
        this.snack.openSnackBar(err.error, 'Close')
      },
    })
  }
  ngOnInit(): void {
  }

  getConnectionProfilesAndIps(selectedRegion: string) {
    this.fetch.getConnectionProfiles(selectedRegion, this.isSource).subscribe({
      next: (res: IConnectionProfile[]) => {
        this.profileList = res
      },
      error: (err: any) => {
        this.snack.openSnackBar(err.error, 'Close')
      },
    })
    if (!this.isSource) {
      this.fetch.getStaticIps(selectedRegion).subscribe({
        next: (res: string[]) => {
          this.ipList = res
        },
        error: (err: any) => {
          this.snack.openSnackBar(err.error, 'Close')
        },

      })
    }
    this.isRegionSelected = true
  }



}
