import { HttpClientModule } from '@angular/common/http';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MatInputModule } from '@angular/material/input';
import { MatRadioModule } from '@angular/material/radio';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { ConnectionProfileFormComponent } from './connection-profile-form.component';

describe('ConnectionProfileFormComponent', () => {
  let component: ConnectionProfileFormComponent;
  let fixture: ComponentFixture<ConnectionProfileFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ConnectionProfileFormComponent ],
      imports: [HttpClientModule, MatSnackBarModule, ReactiveFormsModule, MatRadioModule, MatInputModule, BrowserAnimationsModule],
      providers: [
        {
          provide: MatDialogRef,
          useValue: {
            close: () => {},
          },
        },
        {
          provide: MAT_DIALOG_DATA,
          useValue: {
          }
        }
      ],
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ConnectionProfileFormComponent);
    component = fixture.componentInstance;
    component.isSource = true
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
