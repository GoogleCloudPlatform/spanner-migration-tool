import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { GcsMetadataDetailsFormComponent } from './gcs-metadata-details-form.component';
import { TargetDetails, MigrationDetails } from 'src/app/app.constants';

describe('GcsMetadataDetailsFormComponent', () => {
  let component: GcsMetadataDetailsFormComponent;
  let fixture: ComponentFixture<GcsMetadataDetailsFormComponent>;
  let dialogRefSpy: jasmine.SpyObj<MatDialogRef<GcsMetadataDetailsFormComponent>>;

  beforeEach(async () => {
    dialogRefSpy = jasmine.createSpyObj('MatDialogRef', ['close']);

    await TestBed.configureTestingModule({
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

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should update localStorage and close the dialog', () => {
    spyOn(localStorage, 'setItem');
    component.gcsMetadataDetailsForm.setValue({
      gcsName: 'testGcsName',
      gcsRootPath: 'testGcsRootPath'
    });

    component.updateGcsPathMetadataDetails();

    expect(localStorage.setItem).toHaveBeenCalledWith(TargetDetails.GcsMetadataName, 'testGcsName');
    expect(localStorage.setItem).toHaveBeenCalledWith(TargetDetails.GcsMetadataRootPath, 'testGcsRootPath');
    expect(localStorage.setItem).toHaveBeenCalledWith(MigrationDetails.IsGcsMetadataPathSet, 'true');
    expect(dialogRefSpy.close).toHaveBeenCalled();
  });
});
