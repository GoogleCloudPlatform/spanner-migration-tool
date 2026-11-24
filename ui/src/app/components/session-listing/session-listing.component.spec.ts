import { ComponentFixture, TestBed } from '@angular/core/testing'
import { Router, RouterModule, Routes } from '@angular/router'
import { SessionListingComponent } from './session-listing.component'
import { provideHttpClient, withInterceptorsFromDi } from '@angular/common/http'
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { WorkspaceComponent } from '../workspace/workspace.component'
import { MatSnackBarModule } from '@angular/material/snack-bar'
import { InputType, StorageKeys, TargetDetails } from 'src/app/app.constants'
import IConv from 'src/app/model/conv'

describe('SessionListingComponent', () => {
  let component: SessionListingComponent
  let fixture: ComponentFixture<SessionListingComponent>
  const appRoutes: Routes = [{ path: 'workspace', component: WorkspaceComponent }]

  beforeEach(async () => {
    await TestBed.configureTestingModule({
    declarations: [SessionListingComponent],
    imports: [RouterModule.forRoot(appRoutes), MatSnackBarModule],
    providers: [provideHttpClient(withInterceptorsFromDi()), provideHttpClientTesting()]
}).compileComponents()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(SessionListingComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
  })

  afterEach(() => {
    localStorage.clear()
    vi.restoreAllMocks()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })

  it('should clear stale localStorage before resuming a saved session', () => {
    const router = TestBed.inject(Router)
    const navigateSpy = vi.spyOn(router, 'navigate').mockResolvedValue(true)
    localStorage.setItem(TargetDetails.DefaultTransactionIsolation, 'repeatable read')
    const clearSpy = vi.spyOn(Storage.prototype, 'clear')
    vi.spyOn((component as any).data, 'getSchemaConversionFromResumeSession').mockImplementation(() => {
      ;(component as any).data.convSubject.next({ SpSchema: {} } as IConv)
    })

    component.resumeFromSessionFile('v-123')

    expect(clearSpy).toHaveBeenCalled()
    expect(localStorage.getItem(TargetDetails.DefaultTransactionIsolation)).toBeNull()
    expect(localStorage.getItem(StorageKeys.Config)).toEqual('v-123')
    expect(localStorage.getItem(StorageKeys.Type)).toEqual(InputType.ResumeSession)
    expect(navigateSpy).toHaveBeenCalledWith(['/workspace'])
  })
})
