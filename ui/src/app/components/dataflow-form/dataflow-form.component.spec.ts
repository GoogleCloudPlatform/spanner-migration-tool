import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DataflowFormComponent } from './dataflow-form.component';

describe('DataflowFormComponent', () => {
  let component: DataflowFormComponent;
  let fixture: ComponentFixture<DataflowFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ DataflowFormComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DataflowFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
