import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MatSnackBarModule } from '@angular/material/snack-bar';

import { GcsMetadataDetailsFormComponent } from './gcs-metadata-details-form.component';

describe('GcsMetadataDetailsFormComponent', () => {
  let component: GcsMetadataDetailsFormComponent;
  let fixture: ComponentFixture<GcsMetadataDetailsFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ GcsMetadataDetailsFormComponent ],
      imports: [ReactiveFormsModule, MatSnackBarModule],
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
    fixture = TestBed.createComponent(GcsMetadataDetailsFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
