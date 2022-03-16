import { ComponentFixture, TestBed } from '@angular/core/testing'
import { HttpClientModule } from '@angular/common/http'
import { LoadDumpComponent } from './load-dump.component'
import { RouterModule, Routes } from '@angular/router'
import { WorkspaceComponent } from '../workspace/workspace.component'
const appRoutes: Routes = [{ path: 'workspace', component: WorkspaceComponent }]

describe('LoadDumpComponent', () => {
  let component: LoadDumpComponent
  let fixture: ComponentFixture<LoadDumpComponent>

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [LoadDumpComponent],
      imports: [RouterModule.forRoot(appRoutes), HttpClientModule],
    }).compileComponents()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(LoadDumpComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })
})
