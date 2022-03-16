import { ComponentFixture, TestBed } from '@angular/core/testing'
import { RouterModule, Routes } from '@angular/router'
import { WorkspaceComponent } from '../workspace/workspace.component'
import { HttpClientModule } from '@angular/common/http'
import { DirectConnectionComponent } from './direct-connection.component'

const appRoutes: Routes = [{ path: 'workspace', component: WorkspaceComponent }]

describe('DirectConnectionComponent', () => {
  let component: DirectConnectionComponent
  let fixture: ComponentFixture<DirectConnectionComponent>

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [DirectConnectionComponent],
      imports: [RouterModule.forRoot(appRoutes), HttpClientModule],
    }).compileComponents()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(DirectConnectionComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })
})
