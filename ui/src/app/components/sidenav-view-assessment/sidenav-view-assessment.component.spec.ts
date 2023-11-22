import { HttpClientModule } from '@angular/common/http';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { MatMenuModule } from '@angular/material/menu';
import { MatSnackBarModule } from '@angular/material/snack-bar';

import { SidenavViewAssessmentComponent } from './sidenav-view-assessment.component';

describe('SidenavViewAssessmentComponent', () => {
  let component: SidenavViewAssessmentComponent;
  let fixture: ComponentFixture<SidenavViewAssessmentComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ SidenavViewAssessmentComponent ],
      imports: [ HttpClientModule, MatSnackBarModule, MatMenuModule ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SidenavViewAssessmentComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
