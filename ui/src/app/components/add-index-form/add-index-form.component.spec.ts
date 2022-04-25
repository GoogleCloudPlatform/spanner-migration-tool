import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AddIndexFormComponent } from './add-index-form.component';

describe('AddIndexFormComponent', () => {
  let component: AddIndexFormComponent;
  let fixture: ComponentFixture<AddIndexFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ AddIndexFormComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AddIndexFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
