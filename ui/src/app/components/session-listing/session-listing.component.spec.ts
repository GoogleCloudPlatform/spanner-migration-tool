import { ComponentFixture, TestBed } from '@angular/core/testing'
import { RouterModule, Routes } from '@angular/router'
import { SessionListingComponent } from './session-listing.component'
import { provideHttpClient, withInterceptorsFromDi } from '@angular/common/http'
import { WorkspaceComponent } from '../workspace/workspace.component'
import { MatSnackBarModule } from '@angular/material/snack-bar'

describe('SessionListingComponent', () => {
  let component: SessionListingComponent
  let fixture: ComponentFixture<SessionListingComponent>
  const appRoutes: Routes = [{ path: 'workspace', component: WorkspaceComponent }]

  beforeEach(async () => {
    await TestBed.configureTestingModule({
    declarations: [SessionListingComponent],
    imports: [RouterModule.forRoot(appRoutes), MatSnackBarModule],
    providers: [provideHttpClient(withInterceptorsFromDi())]
}).compileComponents()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(SessionListingComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })
})
