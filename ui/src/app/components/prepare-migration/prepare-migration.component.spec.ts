import { HttpClientModule } from '@angular/common/http';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { MatDialog, MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { of } from 'rxjs';

import { PrepareMigrationComponent } from './prepare-migration.component';
import { TargetDetails, MigrationDetails } from 'src/app/app.constants';
import { GcsMetadataDetailsFormComponent } from '../gcs-metadata-details-form/gcs-metadata-details-form.component';

describe('PrepareMigrationComponent', () => {
  let component: PrepareMigrationComponent;
  let fixture: ComponentFixture<PrepareMigrationComponent>;
  let dialog: MatDialog;
  let dialogRefSpy: jasmine.SpyObj<MatDialogRef<GcsMetadataDetailsFormComponent>>;

  beforeEach(async () => {
    dialogRefSpy = jasmine.createSpyObj('MatDialogRef', ['afterClosed']);

    await TestBed.configureTestingModule({
      declarations: [PrepareMigrationComponent],
      imports: [MatDialogModule, HttpClientModule, MatSnackBarModule],
      providers: [
        { provide: MatDialogRef, useValue: dialogRefSpy },
        { provide: MatDialog, useValue: { open: () => dialogRefSpy } }
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(PrepareMigrationComponent);
    component = fixture.componentInstance;
    dialog = TestBed.inject(MatDialog);

    dialogRefSpy.afterClosed.and.returnValue(of(true));
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PrepareMigrationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should open the dialog and set values after closed', () => {
    const mockValues: { [key: string]: string } = {
      [TargetDetails.TargetDB]: 'mockTargetDB',
      [TargetDetails.SourceConnProfile]: 'mockSourceConnProfile',
      [TargetDetails.TargetConnProfile]: 'mockTargetConnProfile',
      [TargetDetails.ReplicationSlot]: 'mockReplicationSlot',
      [TargetDetails.Publication]: 'mockPublication',
      [TargetDetails.GcsMetadataName]: 'mockGcsMetadataName',
      [TargetDetails.GcsMetadataRootPath]: 'mockGcsMetadataRootPath',
      [MigrationDetails.IsGcsMetadataPathSet]: 'true'
    };

    spyOn(localStorage, 'getItem').and.callFake((key: string) => mockValues[key]);
    const dialogOpenSpy = spyOn(dialog, 'open').and.returnValue(dialogRefSpy);

    component.openGcsMetadataDetailsForm();

    expect(dialogOpenSpy).toHaveBeenCalledWith(GcsMetadataDetailsFormComponent, {
      width: '30vw',
      minWidth: '400px',
      maxWidth: '500px',
    });

    dialogRefSpy.afterClosed().subscribe(() => {
      expect(component.isGcsMetadataDetailSet).toBeTrue();
      expect(component.targetDetails).toEqual({
        TargetDB: 'mockTargetDB',
        SourceConnProfile: 'mockSourceConnProfile',
        TargetConnProfile: 'mockTargetConnProfile',
        ReplicationSlot: 'mockReplicationSlot',
        Publication: 'mockPublication',
        GcsMetadataPath: {
          GcsBucketName: 'mockGcsMetadataName',
          GcsBucketRootPath: 'mockGcsMetadataRootPath',
        },
      });
    });
  });
});
