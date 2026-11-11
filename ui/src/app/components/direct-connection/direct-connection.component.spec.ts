import { ComponentFixture, TestBed } from '@angular/core/testing'
import { RouterModule, Routes } from '@angular/router'
import { WorkspaceComponent } from '../workspace/workspace.component'
import { HttpResponse, provideHttpClient, withInterceptorsFromDi } from '@angular/common/http'
import { DirectConnectionComponent } from './direct-connection.component'
import { By } from '@angular/platform-browser'
import { ReactiveFormsModule } from '@angular/forms'
import { MatCardModule } from '@angular/material/card'
import { MatSelectModule } from '@angular/material/select'
import { MatOptionModule } from '@angular/material/core'
import { MatFormFieldModule } from '@angular/material/form-field'
import { MatInputModule } from '@angular/material/input'
import { BrowserAnimationsModule } from '@angular/platform-browser/animations'
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service'
import { MatSnackBarModule } from '@angular/material/snack-bar'
import { MatDialog } from '@angular/material/dialog'
import { DataService } from 'src/app/services/data/data.service'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { PersistedFormValues } from 'src/app/app.constants'
import { of } from 'rxjs'

const appRoutes: Routes = [{ path: 'workspace', component: WorkspaceComponent }]

describe('DirectConnectionComponent', () => {
  let component: DirectConnectionComponent
  let fixture: ComponentFixture<DirectConnectionComponent>
  let btn: any
  beforeEach(async () => {
    await TestBed.configureTestingModule({
    declarations: [DirectConnectionComponent],
    imports: [RouterModule.forRoot(appRoutes),
        ReactiveFormsModule,
        MatCardModule,
        MatSelectModule,
        MatOptionModule,
        MatFormFieldModule,
        MatInputModule,
        BrowserAnimationsModule,
        MatSnackBarModule],
    providers: [
        SnackbarService,
        { provide: MatDialog, useValue: {} },
        provideHttpClient(withInterceptorsFromDi())
    ]
}).compileComponents()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(DirectConnectionComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
    btn = fixture.debugElement.query(By.css('button[type=submit]'))
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })

  it('should validate required input', () => {
    let engine = component.connectForm.get('dbEngine')
    engine?.setValue('')

    expect(component.connectForm.valid).toBeFalsy()

    expect(engine?.invalid).toBeTruthy()
    engine?.setValue('mysql')
    expect(engine?.invalid).toBeFalsy()

    let host = component.connectForm.get('hostName')
    host?.setValue('')
    expect(host?.invalid).toBeTruthy()
    host?.setValue('104.45.67.00')
    expect(host?.invalid).toBeFalsy()

    let port = component.connectForm.get('port')
    port?.setValue('')
    expect(port?.invalid).toBeTruthy()
    port?.setValue('1433')
    expect(port?.invalid).toBeFalsy()

    let user = component.connectForm.get('userName')
    user?.setValue('')
    expect(user?.invalid).toBeTruthy()
    user?.setValue('sa')
    expect(user?.invalid).toBeFalsy()

    let pass = component.connectForm.get('password')
    pass?.setValue('')
    expect(pass?.invalid).toBeFalsy()
    pass?.setValue('23143')
    expect(pass?.invalid).toBeFalsy()

    let dbname = component.connectForm.get('dbName')
    dbname?.setValue('')
    expect(dbname?.invalid).toBeTruthy()
    dbname?.setValue('mysql')
    expect(dbname?.invalid).toBeFalsy()

    let dialect = component.connectForm.get('dialect')
    dialect?.setValue('')
    expect(dialect?.invalid).toBeTruthy()
    dialect?.setValue('postgresql')
    expect(dialect?.invalid).toBeFalsy()

    // now every input is valid do form will be valid
    expect(component.connectForm.valid).toBeTruthy()
  })

  it('should button disabled when form is invalid', () => {
    let host = component.connectForm.get('hostName')
    host?.setValue('')

    expect(component.connectForm.invalid).toBeTruthy()
    fixture.detectChanges()

    expect(btn.nativeElement.disabled).toEqual(true)
  })

  it('should button enabled when form is valid', () => {
    let host = component.connectForm.get('hostName')
    host?.setValue('')
    expect(component.connectForm.invalid).toBeTruthy()

    component.connectForm.patchValue({
      hostName: 'localhost',
      dbEngine: 'mssql',
      port: '1433',
      userName: 'sa',
      password: 'password',
      dbName: 'database',
      dialect: 'postgresql',
    })
    expect(component.connectForm.valid).toBeTruthy()
    fixture.detectChanges()
    expect(btn.nativeElement.disabled).toBeFalsy()
  })

  describe('Cassandra Engine Tests', () => {
    beforeEach(() => {
      component.connectForm.get('dbEngine')?.setValue('cassandra')
      fixture.detectChanges()
    })

    it('should have an invalid form if dataCenter is missing', () => {
      component.connectForm.patchValue({
        hostName: 'localhost',
        userName: 'user',
        dbName: 'keyspace1'
      })
      
      const dataCenter = component.connectForm.get('dataCenter')
      dataCenter?.setValue('')

      expect(component.connectForm.valid).toBeFalsy()
    })

    it('should have a valid form and enabled button with all required fields', () => {
        component.connectForm.patchValue({
            hostName: 'localhost',
            port: '9042',
            userName: 'user',
            password: 'pass',
            dbName: 'keyspace1',
            dataCenter: 'dc1',
            dialect: 'google_standard_sql',
        })

        fixture.detectChanges()

        expect(component.connectForm.valid).toBeTruthy()
        expect(btn.nativeElement.disabled).toBeFalsy()
    })
  })

  it('should not store password in localStorage on testConn and should restore password from DataService on ngOnInit', () => {
    const dataService = TestBed.inject(DataService)
    const fetchService = TestBed.inject(FetchService)
    const snackbarService = TestBed.inject(SnackbarService)
    spyOn(fetchService, 'connectTodb').and.returnValue(of(new HttpResponse<HttpResponse<null>>({ status: 200 })))
    spyOn(snackbarService, 'openSnackBar')
    localStorage.clear()
    component.connectForm.patchValue({
      dbEngine: 'mysql',
      isSharded: false,
      hostName: 'localhost',
      port: '3306',
      userName: 'root',
      password: 'my-secret-password',
      dbName: 'testdb',
      dialect: 'google_standard_sql',
      dataCenter: '',
    })

    component.testConn()

    const storedForm = JSON.parse(localStorage.getItem(PersistedFormValues.DirectConnectForm)!)
    expect(storedForm.password).toBeUndefined()
    expect(dataService.getDirectConnectPassword()).toEqual('my-secret-password')

    component.connectForm.patchValue({ password: '' })
    component.ngOnInit()
    expect(component.connectForm.get('password')?.value).toEqual('my-secret-password')
    localStorage.clear()
    dataService.resetStore()
  })
})
