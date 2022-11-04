import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SourceDetailsFormComponent } from './source-details-form.component';

describe('SourceDetailsFormComponent', () => {
  let component: SourceDetailsFormComponent;
  let fixture: ComponentFixture<SourceDetailsFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ SourceDetailsFormComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SourceDetailsFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
