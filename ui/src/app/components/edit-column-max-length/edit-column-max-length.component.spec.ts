import { ComponentFixture, TestBed } from '@angular/core/testing';

import { EditColumnMaxLengthComponent } from './edit-column-max-length.component';

describe('EditColumnMaxLengthComponent', () => {
  let component: EditColumnMaxLengthComponent;
  let fixture: ComponentFixture<EditColumnMaxLengthComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ EditColumnMaxLengthComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(EditColumnMaxLengthComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
