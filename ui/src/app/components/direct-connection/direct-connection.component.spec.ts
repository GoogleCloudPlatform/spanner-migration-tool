import { ComponentFixture, TestBed } from '@angular/core/testing'
import { RouterModule, Routes } from '@angular/router'
import { WorkspaceComponent } from '../workspace/workspace.component'
import { HttpClientModule } from '@angular/common/http'
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

const appRoutes: Routes = [{ path: 'workspace', component: WorkspaceComponent }]

describe('DirectConnectionComponent', () => {
  let component: DirectConnectionComponent
  let fixture: ComponentFixture<DirectConnectionComponent>
  let btn: any
  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [DirectConnectionComponent],
      imports: [
        RouterModule.forRoot(appRoutes),
        HttpClientModule,
        ReactiveFormsModule,
        MatCardModule,
        MatSelectModule,
        MatOptionModule,
        MatFormFieldModule,
        MatInputModule,
        BrowserAnimationsModule,
        MatSnackBarModule,
      ],
      providers: [SnackbarService],
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
    expect(pass?.invalid).toBeTruthy()
    pass?.setValue('23143')
    expect(pass?.invalid).toBeFalsy()

    let dbname = component.connectForm.get('dbName')
    dbname?.setValue('')
    expect(dbname?.invalid).toBeTruthy()
    dbname?.setValue('mysql')
    expect(dbname?.invalid).toBeFalsy()

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
    })
    expect(component.connectForm.valid).toBeTruthy()
    fixture.detectChanges()
    expect(btn.nativeElement.disabled).toBeFalsy()
  })
})
