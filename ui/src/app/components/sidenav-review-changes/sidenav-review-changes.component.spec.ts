import { NO_ERRORS_SCHEMA } from '@angular/core';
import { provideHttpClient, withInterceptorsFromDi } from '@angular/common/http'
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing'
import { MatSnackBarModule } from '@angular/material/snack-bar'

import { SidenavReviewChangesComponent } from './sidenav-review-changes.component'

describe('SidenavReviewChangesComponent', () => {
  let component: SidenavReviewChangesComponent
  let fixture: ComponentFixture<SidenavReviewChangesComponent>

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      schemas: [NO_ERRORS_SCHEMA],
    declarations: [SidenavReviewChangesComponent],
    imports: [MatSnackBarModule],
    providers: [provideHttpClient(withInterceptorsFromDi()), provideHttpClientTesting()]
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
