import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TargetDetailsFormComponent } from './target-details-form.component';

describe('TargetDetailsFormComponent', () => {
  let component: TargetDetailsFormComponent;
  let fixture: ComponentFixture<TargetDetailsFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ TargetDetailsFormComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TargetDetailsFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
