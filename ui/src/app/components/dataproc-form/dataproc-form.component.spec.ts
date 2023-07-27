import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DataprocFormComponent } from './dataproc-form.component';

describe('DataprocFormComponent', () => {
  let component: DataprocFormComponent;
  let fixture: ComponentFixture<DataprocFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ DataprocFormComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DataprocFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
