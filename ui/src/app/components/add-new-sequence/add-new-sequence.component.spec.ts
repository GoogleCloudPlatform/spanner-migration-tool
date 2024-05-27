import { HttpClientModule } from '@angular/common/http';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormBuilder, ReactiveFormsModule } from '@angular/forms';
import { MatDialogModule, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { AddNewSequenceComponent } from './add-new-sequence.component';
import { DataService } from 'src/app/services/data/data.service';

describe('AddNewSequenceComponent', () => {
  let component: AddNewSequenceComponent;
  let fixture: ComponentFixture<AddNewSequenceComponent>;
  let dataServiceSpy: jasmine.SpyObj<DataService>;

  beforeEach(async () => {
    dataServiceSpy = jasmine.createSpyObj('DataService', ['addSequence']);
    await TestBed.configureTestingModule({
      declarations: [AddNewSequenceComponent],
      imports: [ReactiveFormsModule, HttpClientModule, MatSnackBarModule, MatDialogModule, MatSelectModule, MatInputModule, BrowserAnimationsModule],
      providers: [
        {
          provide: MatDialogRef,
          useValue: {
            close: () => { },
          },
        },
        {
          provide: DataService,
          useValue: dataServiceSpy
        }
      ],
    })
      .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AddNewSequenceComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('add new sequence', () => {
    let formBuilder = new FormBuilder()
      component.addNewSequenceForm = formBuilder.group({})
      component.addNewSequence()
      expect(dataServiceSpy.addSequence).toHaveBeenCalled()
  });
});
