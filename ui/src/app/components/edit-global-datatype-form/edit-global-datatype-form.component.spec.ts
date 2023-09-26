import { HttpClientModule } from '@angular/common/http';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { EditGlobalDatatypeFormComponent } from './edit-global-datatype-form.component';

describe('EditGlobalDatatypeFormComponent', () => {
  let component: EditGlobalDatatypeFormComponent;
  let fixture: ComponentFixture<EditGlobalDatatypeFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ EditGlobalDatatypeFormComponent ],
      imports: [ ReactiveFormsModule, HttpClientModule, MatSnackBarModule, MatSelectModule, MatInputModule, BrowserAnimationsModule ]
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
