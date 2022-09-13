import { Component, OnInit } from '@angular/core';
import { FormBuilder, FormGroup } from '@angular/forms';
import IConnectionProfile from 'src/app/model/profile';
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
  constructor(
    private fetch: FetchService,
    private snack: SnackbarService,
    private formBuilder: FormBuilder,
  ) {
    this.connectionProfileForm = this.formBuilder.group({})
  }

  ngOnInit(): void {
  }

  getConnectionProfile(selectedRegion: string) {
    this.fetch.getSourceConnectionProfiles(selectedRegion).subscribe({
      next: (res: IConnectionProfile[]) => {
        console.log(res)
        this.profileList = res
        let createConnection: IConnectionProfile = {
          Name: "create-new-profile",
          DisplayName: "Create New Connection profile"
        }
        this.profileList.push(createConnection)
        console.log(this.profileList)
      },
      error: (err: any) => {
        this.snack.openSnackBar(err.error, 'Close')
      },

    })
    this.isRegionSelected = true
  }

}
