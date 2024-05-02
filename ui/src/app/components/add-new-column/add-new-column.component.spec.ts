import { HttpClientModule } from '@angular/common/http';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { MatDialogModule, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { Dialect } from 'src/app/app.constants';

import { AddNewColumnComponent } from './add-new-column.component';

describe('AddNewColumnComponent', () => {
  let component: AddNewColumnComponent;
  let fixture: ComponentFixture<AddNewColumnComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [AddNewColumnComponent],
      imports: [ReactiveFormsModule, HttpClientModule, MatSnackBarModule, MatDialogModule, MatSelectModule, MatInputModule, BrowserAnimationsModule],
      providers: [
        {
          provide: MatDialogRef,
          useValue: {
            close: () => { },
          },
        },
        {
          provide: MAT_DIALOG_DATA,
          useValue: {}
        }
      ],
    })
      .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AddNewColumnComponent);
    component = fixture.componentInstance;
    component.dialect = Dialect.GoogleStandardSQLDialect;
    console.log(component.datatypes)
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
