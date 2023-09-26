import { HttpClientModule } from '@angular/common/http';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { EditColumnMaxLengthComponent } from './edit-column-max-length.component';

describe('EditColumnMaxLengthComponent', () => {
  let component: EditColumnMaxLengthComponent;
  let fixture: ComponentFixture<EditColumnMaxLengthComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ EditColumnMaxLengthComponent ],
      imports: [ReactiveFormsModule, HttpClientModule, MatSnackBarModule, MatInputModule, MatSelectModule, BrowserAnimationsModule]
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
