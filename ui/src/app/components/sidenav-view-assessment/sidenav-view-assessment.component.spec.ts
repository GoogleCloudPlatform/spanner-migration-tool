import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SidenavViewAssessmentComponent } from './sidenav-view-assessment.component';

describe('SidenavViewAssessmentComponent', () => {
  let component: SidenavViewAssessmentComponent;
  let fixture: ComponentFixture<SidenavViewAssessmentComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ SidenavViewAssessmentComponent ]
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
