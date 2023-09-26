import { HttpClientModule } from '@angular/common/http'
import { ComponentFixture, TestBed } from '@angular/core/testing'
import { MatSnackBarModule } from '@angular/material/snack-bar'

import { SidenavReviewChangesComponent } from './sidenav-review-changes.component'

describe('SidenavReviewChangesComponent', () => {
  let component: SidenavReviewChangesComponent
  let fixture: ComponentFixture<SidenavReviewChangesComponent>

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [SidenavReviewChangesComponent],
      imports: [ HttpClientModule, MatSnackBarModule ]
    }).compileComponents()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(SidenavReviewChangesComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })
})
