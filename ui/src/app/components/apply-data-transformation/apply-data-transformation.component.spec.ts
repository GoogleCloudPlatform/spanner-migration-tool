import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ApplyDataTransformationComponent } from './apply-data-transformation.component';

describe('ApplyDataTransformationComponent', () => {
  let component: ApplyDataTransformationComponent;
  let fixture: ComponentFixture<ApplyDataTransformationComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ApplyDataTransformationComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ApplyDataTransformationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
