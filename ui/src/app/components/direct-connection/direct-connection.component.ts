import { Component, OnInit } from '@angular/core'
import { FormControl, FormGroup, Validators } from '@angular/forms'
import { Router } from '@angular/router'
import IDbConfig from 'src/app/model/db-config'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { DataService } from 'src/app/services/data/data.service'
import { LoaderService } from '../../services/loader/loader.service'
import { DialectList, InputType, PersistedFormValues, StorageKeys } from 'src/app/app.constants'
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service'
import { extractSourceDbName } from 'src/app/utils/utils'
import { ClickEventService } from 'src/app/services/click-event/click-event.service'

@Component({
  selector: 'app-direct-connection',
  templateUrl: './direct-connection.component.html',
  styleUrls: ['./direct-connection.component.scss'],
})
export class DirectConnectionComponent implements OnInit {
  connectForm = new FormGroup({
    dbEngine: new FormControl('', [Validators.required]),
    isSharded: new FormControl(false),
    hostName: new FormControl('', [Validators.required]),
    port: new FormControl('', [Validators.required, Validators.pattern('^[0-9]+$')]),
    userName: new FormControl('', [Validators.required]),
    password: new FormControl(''),
    dbName: new FormControl('', [Validators.required]),
    dialect: new FormControl('', [Validators.required]),
  })

  dbEngineList = [
    { value: 'mysql', displayName: 'MySQL' },
    { value: 'sqlserver', displayName: 'SQL Server' },
    { value: 'oracle', displayName: 'Oracle' },
    { value: 'postgres', displayName: 'PostgreSQL' },
  ]

  isTestConnectionSuccessful = false

  connectRequest: any = null
  getSchemaRequest: any = null
  shardedResponseList = [
    { value: false, displayName: 'No'},
    { value: true, displayName: 'Yes'},
  ]

  dialect = DialectList

  constructor(
    private router: Router,
    private fetch: FetchService,
    private data: DataService,
    private loader: LoaderService,
    private snackbarService: SnackbarService,
    private clickEvent: ClickEventService
  ) {}

  ngOnInit(): void {
    //initialise component with the previously persisted values if present.
    if (localStorage.getItem(PersistedFormValues.DirectConnectForm) != null) {
      this.connectForm.setValue(JSON.parse(localStorage.getItem(PersistedFormValues.DirectConnectForm) as string))
    }
    if (localStorage.getItem(PersistedFormValues.IsConnectionSuccessful) != null) {
      this.isTestConnectionSuccessful = localStorage.getItem(PersistedFormValues.IsConnectionSuccessful) === 'true'
    }
    this.clickEvent.cancelDbLoad.subscribe({
      next: (res: boolean) => {
        if (res && this.connectRequest) {
          this.connectRequest.unsubscribe()
          if (this.getSchemaRequest) {
            this.getSchemaRequest.unsubscribe()
          }
        }
      },
    })
  }

  testConn() {
    this.clickEvent.openDatabaseLoader('test-connection', this.connectForm.value.dbName)
    const { dbEngine, isSharded, hostName, port, userName, password, dbName, dialect } = this.connectForm.value
    localStorage.setItem(PersistedFormValues.DirectConnectForm, JSON.stringify(this.connectForm.value))
    const config: IDbConfig = { dbEngine, isSharded, hostName, port, userName, password, dbName }
    this.connectRequest =this.fetch.connectTodb(config, dialect).subscribe({
      next: () => {
        this.snackbarService.openSnackBar('SUCCESS! Spanner migration tool was able to successfully ping source database', 'Close', 3)
        //Datbase loader causes the direct connection form to get refreshed hence this value needs to be persisted to local storage.
        localStorage.setItem(PersistedFormValues.IsConnectionSuccessful, "true")
        this.clickEvent.closeDatabaseLoader()
      },
      error: (e) => { 
        this.isTestConnectionSuccessful = false
        this.snackbarService.openSnackBar(e.error, 'Close')
        localStorage.setItem(PersistedFormValues.IsConnectionSuccessful, "false")
        this.clickEvent.closeDatabaseLoader()
      }
    })
  }

  connectToDb() {
    this.clickEvent.openDatabaseLoader('direct', this.connectForm.value.dbName)
    window.scroll(0, 0)
    this.data.resetStore()
    localStorage.clear()
    const { dbEngine, isSharded, hostName, port, userName, password, dbName, dialect } = this.connectForm.value
    localStorage.setItem(PersistedFormValues.DirectConnectForm, JSON.stringify(this.connectForm.value))
    const config: IDbConfig = { dbEngine, isSharded, hostName, port, userName, password, dbName }
    this.connectRequest =this.fetch.connectTodb(config, dialect).subscribe({
      next: () => {
        this.getSchemaRequest = this.data.getSchemaConversionFromDb()
        this.data.conv.subscribe((res) => {
          localStorage.setItem(
            StorageKeys.Config,
            JSON.stringify({ dbEngine, hostName, port, userName, password, dbName })
          )
          localStorage.setItem(StorageKeys.Type, InputType.DirectConnect)
          localStorage.setItem(StorageKeys.SourceDbName, extractSourceDbName(dbEngine))
          this.clickEvent.closeDatabaseLoader()
          //after a successful load, remove the persisted values.
          localStorage.removeItem(PersistedFormValues.DirectConnectForm)
          this.router.navigate(['/workspace'])
        })
      },
      error: (e) => { 
        this.snackbarService.openSnackBar(e.error, 'Close')
        this.clickEvent.closeDatabaseLoader()
      },
    })
  }

  refreshDbSpecifcConnectionOptions() {
    this.connectForm.value.isSharded = false
  }
}
