import { HttpClientModule } from '@angular/common/http'
import { ComponentFixture, TestBed } from '@angular/core/testing'
import { MatSnackBarModule } from '@angular/material/snack-bar'

import { SidenavSaveSessionComponent } from './sidenav-save-session.component'

describe('SidenavSaveSessionComponent', () => {
  let component: SidenavSaveSessionComponent
  let fixture: ComponentFixture<SidenavSaveSessionComponent>

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [SidenavSaveSessionComponent],
      imports: [ HttpClientModule, MatSnackBarModule ]
    }).compileComponents()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(SidenavSaveSessionComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })
})
