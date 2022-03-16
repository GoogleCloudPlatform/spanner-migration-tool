import { HttpClientModule } from '@angular/common/http'
import { ComponentFixture, TestBed } from '@angular/core/testing'
import { RouterModule, Routes } from '@angular/router'
import { WorkspaceComponent } from '../workspace/workspace.component'
import { LoadSessionComponent } from './load-session.component'

const appRoutes: Routes = [{ path: 'workspace', component: WorkspaceComponent }]

describe('LoadSessionComponent', () => {
  let component: LoadSessionComponent
  let fixture: ComponentFixture<LoadSessionComponent>

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [LoadSessionComponent],
      imports: [RouterModule.forRoot(appRoutes), HttpClientModule],
    }).compileComponents()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(LoadSessionComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })
})
