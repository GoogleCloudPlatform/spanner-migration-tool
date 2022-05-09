import { Component, OnInit } from '@angular/core'
import { FormControl, FormGroup, Validators } from '@angular/forms'
import { Router } from '@angular/router'
import IDbConfig from 'src/app/model/DbConfig'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { DataService } from 'src/app/services/data/data.service'
import { LoaderService } from '../../services/loader/loader.service'
import { InputType, StorageKeys } from 'src/app/app.constants'
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service'
import { extractSourceDbName } from 'src/app/utils/utils'

@Component({
  selector: 'app-direct-connection',
  templateUrl: './direct-connection.component.html',
  styleUrls: ['./direct-connection.component.scss'],
})
export class DirectConnectionComponent implements OnInit {
  connectForm = new FormGroup({
    dbEngine: new FormControl('', [Validators.required]),
    hostName: new FormControl('', [Validators.required]),
    port: new FormControl('', [Validators.required]),
    userName: new FormControl('', [Validators.required]),
    password: new FormControl('', [Validators.required]),
    dbName: new FormControl('', [Validators.required]),
  })

  constructor(
    private router: Router,
    private fetch: FetchService,
    private data: DataService,
    private loader: LoaderService,
    private snackbarService: SnackbarService
  ) {}

  ngOnInit(): void {}

  connectToDb() {
    window.scroll(0, 0)
    this.data.resetStore()
    const { dbEngine, hostName, port, userName, password, dbName } = this.connectForm.value
    const config: IDbConfig = { dbEngine, hostName, port, userName, password, dbName }
    this.fetch.connectTodb(config).subscribe({
      next: (res) => {
        if (res.status == 200) {
          localStorage.setItem(
            StorageKeys.Config,
            JSON.stringify({ dbEngine, hostName, port, userName, password, dbName })
          )
          localStorage.setItem(StorageKeys.Type, InputType.DirectConnect)
          localStorage.setItem(StorageKeys.SourceDbName, extractSourceDbName(dbEngine))
        }
        this.data.getSchemaConversionFromDb()
        this.data.conv.subscribe((res) => {
          this.router.navigate(['/workspace'])
        })
      },
      error: (e) => {
        this.snackbarService.openSnackBar(e.error, 'Close')
      },
    })
  }
}
