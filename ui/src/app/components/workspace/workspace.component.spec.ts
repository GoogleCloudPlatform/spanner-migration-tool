import { ComponentFixture, TestBed } from '@angular/core/testing'
import { HttpClientModule } from '@angular/common/http'

import { WorkspaceComponent } from './workspace.component'

describe('WorkspaceComponent', () => {
  let component: WorkspaceComponent
  let fixture: ComponentFixture<WorkspaceComponent>

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [WorkspaceComponent],
      imports: [HttpClientModule],
    }).compileComponents()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(WorkspaceComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })
})
