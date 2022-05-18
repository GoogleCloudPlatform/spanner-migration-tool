import { ComponentFixture, TestBed } from '@angular/core/testing';

import { EditGlobalDatatypeFormComponent } from './edit-global-datatype-form.component';

describe('EditGlobalDatatypeFormComponent', () => {
  let component: EditGlobalDatatypeFormComponent;
  let fixture: ComponentFixture<EditGlobalDatatypeFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ EditGlobalDatatypeFormComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(EditGlobalDatatypeFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
