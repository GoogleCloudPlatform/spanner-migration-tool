import { Component, OnInit } from '@angular/core'
import { FormControl, FormGroup, Validators } from '@angular/forms'
import { DataService } from 'src/app/services/data/data.service'
import ISessionConfig from '../../model/SessionConfig'
import { Router } from '@angular/router'
import { InputType, StorageKeys } from 'src/app/app.constants'
import { extractSourceDbName } from 'src/app/utils/utils'
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service'

@Component({
  selector: 'app-load-session',
  templateUrl: './load-session.component.html',
  styleUrls: ['./load-session.component.scss'],
})
export class LoadSessionComponent implements OnInit {
  constructor(
    private data: DataService,
    private router: Router,
    private snackbar: SnackbarService
  ) {}

  connectForm = new FormGroup({
    dbEngine: new FormControl('sqlserver', [Validators.required]),
    filePath: new FormControl('harbour_bridge_output/BikeStores/BikeStores.session.json', [
      Validators.required,
    ]),
  })

  ngOnInit(): void {}

  convertFromSessionFile() {
    this.data.resetStore()
    const { dbEngine, filePath } = this.connectForm.value
    const payload: ISessionConfig = {
      driver: dbEngine,
      filePath: filePath,
    }
    // this.data.initiateSession()
    this.data.getSchemaConversionFromSession(payload)
    // .subscribe((res: string) => {
    //   if (res !== '') {
    //     this.snackbar.openSnackBar('Failed to load session file', 'Dismiss')
    //   }
    // })
    this.data.conv.subscribe((res) => {
      localStorage.setItem(StorageKeys.Config, JSON.stringify(payload))
      localStorage.setItem(StorageKeys.Type, InputType.SessionFile)
      localStorage.setItem(StorageKeys.SourceDbName, extractSourceDbName(dbEngine))
      this.router.navigate(['/workspace'])
    })
  }
}
