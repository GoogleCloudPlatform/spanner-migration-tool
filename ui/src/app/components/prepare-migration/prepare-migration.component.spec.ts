import { provideHttpClient, withInterceptorsFromDi } from '@angular/common/http';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { MatDialog, MatDialogModule, MatDialogRef } from '@angular/material/dialog';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { of } from 'rxjs';

import IConv, { ISessionSummary } from 'src/app/model/conv';
import { PrepareMigrationComponent } from './prepare-migration.component';
import { TargetDetails, MigrationDetails, SourceDbNames, MigrationModes } from 'src/app/app.constants';
import { GcsMetadataDetailsFormComponent } from '../gcs-metadata-details-form/gcs-metadata-details-form.component';
import { FetchService } from 'src/app/services/fetch/fetch.service';
import ISpannerConfig from 'src/app/model/spanner-config';

describe('PrepareMigrationComponent', () => {
  let component: PrepareMigrationComponent;
  let fixture: ComponentFixture<PrepareMigrationComponent>;
  let dialog: MatDialog;
  let dialogRefSpy: jasmine.SpyObj<MatDialogRef<GcsMetadataDetailsFormComponent>>;

  let fetchServiceSpy: jasmine.SpyObj<FetchService>;

  beforeEach(async () => {
    dialogRefSpy = jasmine.createSpyObj('MatDialogRef', ['afterClosed']);
    fetchServiceSpy = jasmine.createSpyObj('FetchService', ['getSourceDestinationSummary', 'getLastSessionDetails', 'getSpannerConfig', 'getIsOffline']);
    fetchServiceSpy.getLastSessionDetails.and.returnValue(of({} as IConv));
    fetchServiceSpy.getSpannerConfig.and.returnValue(of({} as ISpannerConfig));
    fetchServiceSpy.getIsOffline.and.returnValue(of(false));
    fetchServiceSpy.getSourceDestinationSummary.and.returnValue(of({ DatabaseType: SourceDbNames.Cassandra } as ISessionSummary));

    await TestBed.configureTestingModule({
    declarations: [PrepareMigrationComponent],
    imports: [MatDialogModule, MatSnackBarModule],
    providers: [
        { provide: MatDialogRef, useValue: dialogRefSpy },
        { provide: MatDialog, useValue: { open: () => dialogRefSpy } },
        { provide: FetchService, useValue: fetchServiceSpy },
        provideHttpClient(withInterceptorsFromDi())
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

  it('should set migration mode to schemaOnly for Cassandra source', () => {
    const mockSummary = {
      DatabaseType: SourceDbNames.Cassandra,
      SourceTableCount: 10,
      SpannerTableCount: 10,
      SourceIndexCount: 5,
      SpannerIndexCount: 5,
      ConnectionType: 'sessionFile',
      SourceDatabaseName: 'testdb',
    };
    fetchServiceSpy.getSourceDestinationSummary.and.returnValue(of(mockSummary as any));

    component.ngOnInit();

    expect(component.migrationModes).toEqual([MigrationModes.schemaOnly]);
    expect(component.selectedMigrationMode).toEqual(MigrationModes.schemaOnly);
  });
});
