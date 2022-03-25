import { ComponentFixture, TestBed } from '@angular/core/testing'
import { RouterModule, Routes } from '@angular/router'
import { SessionListingComponent } from './session-listing.component'
import { HttpClientModule } from '@angular/common/http'
import { WorkspaceComponent } from '../workspace/workspace.component'

describe('SessionListingComponent', () => {
  let component: SessionListingComponent
  let fixture: ComponentFixture<SessionListingComponent>
  const appRoutes: Routes = [{ path: 'workspace', component: WorkspaceComponent }]

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [SessionListingComponent],
      imports: [HttpClientModule, RouterModule.forRoot(appRoutes)],
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
