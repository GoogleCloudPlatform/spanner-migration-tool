import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { GcsMetadataDetailsFormComponent } from './gcs-metadata-details-form.component';
import { TargetDetails, MigrationDetails } from 'src/app/app.constants';

describe('GcsMetadataDetailsFormComponent', () => {
  let component: GcsMetadataDetailsFormComponent;
  let fixture: ComponentFixture<GcsMetadataDetailsFormComponent>;
  let dialogRefSpy: { close: ReturnType<typeof vi.fn> };

  beforeEach(async () => {
    dialogRefSpy = {
      close: vi.fn(),
    };

    await TestBed.configureTestingModule({
      schemas: [NO_ERRORS_SCHEMA],
      declarations: [GcsMetadataDetailsFormComponent],
      imports: [ReactiveFormsModule, MatSnackBarModule],
      providers: [
        {
          provide: MatDialogRef,
          useValue: dialogRefSpy,
        },
        {
          provide: MAT_DIALOG_DATA,
          useValue: {},
        },
      ],
    }).compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(GcsMetadataDetailsFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should update localStorage and close the dialog', () => {
    const setItemSpy = vi.spyOn(Storage.prototype, 'setItem');
    component.gcsMetadataDetailsForm.setValue({
      gcsName: 'testGcsName',
      gcsRootPath: 'testGcsRootPath'
    });

    component.updateGcsPathMetadataDetails();

    expect(setItemSpy).toHaveBeenCalledWith(TargetDetails.GcsMetadataName, 'testGcsName');
    expect(setItemSpy).toHaveBeenCalledWith(TargetDetails.GcsMetadataRootPath, 'testGcsRootPath');
    expect(setItemSpy).toHaveBeenCalledWith(MigrationDetails.IsGcsMetadataPathSet, 'true');
    expect(dialogRefSpy.close).toHaveBeenCalled();
  });
});
