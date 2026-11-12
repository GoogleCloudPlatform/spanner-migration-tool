import { NO_ERRORS_SCHEMA } from '@angular/core';
import { provideHttpClient, withInterceptorsFromDi } from '@angular/common/http'
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing'
import { MatSnackBarModule } from '@angular/material/snack-bar'

import { SidenavSaveSessionComponent } from './sidenav-save-session.component'

describe('SidenavSaveSessionComponent', () => {
  let component: SidenavSaveSessionComponent
  let fixture: ComponentFixture<SidenavSaveSessionComponent>

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      schemas: [NO_ERRORS_SCHEMA],
    declarations: [SidenavSaveSessionComponent],
    imports: [MatSnackBarModule],
    providers: [provideHttpClient(withInterceptorsFromDi()), provideHttpClientTesting()]
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
