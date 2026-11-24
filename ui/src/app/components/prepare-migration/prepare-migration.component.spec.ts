import { NO_ERRORS_SCHEMA } from '@angular/core';
import { provideHttpClient, withInterceptorsFromDi } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
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
  let dialogRefSpy: { afterClosed: ReturnType<typeof vi.fn<() => any>> };
  let fetchServiceSpy: {
    getSourceDestinationSummary: ReturnType<typeof vi.fn<(...args: any[]) => any>>;
    getLastSessionDetails: ReturnType<typeof vi.fn<(...args: any[]) => any>>;
    getSpannerConfig: ReturnType<typeof vi.fn<(...args: any[]) => any>>;
    getIsOffline: ReturnType<typeof vi.fn<(...args: any[]) => any>>;
    updateDatabaseOptions: ReturnType<typeof vi.fn<(...args: any[]) => any>>;
  };

  beforeEach(async () => {
    dialogRefSpy = {
      afterClosed: vi.fn().mockReturnValue(of(true)),
    };
    fetchServiceSpy = {
      getLastSessionDetails: vi.fn().mockReturnValue(of({} as IConv)),
      getSpannerConfig: vi.fn().mockReturnValue(of({} as ISpannerConfig)),
      getIsOffline: vi.fn().mockReturnValue(of(false)),
      getSourceDestinationSummary: vi.fn().mockReturnValue(of({ DatabaseType: SourceDbNames.Cassandra } as ISessionSummary)),
      updateDatabaseOptions: vi.fn().mockReturnValue(of({})),
    };

    await TestBed.configureTestingModule({
      schemas: [NO_ERRORS_SCHEMA],
      declarations: [PrepareMigrationComponent],
      imports: [MatDialogModule, MatSnackBarModule],
      providers: [
        { provide: MatDialogRef, useValue: dialogRefSpy },
        { provide: MatDialog, useValue: { open: () => dialogRefSpy } },
        { provide: FetchService, useValue: fetchServiceSpy },
        provideHttpClient(withInterceptorsFromDi()), provideHttpClientTesting()
      ]
    }).compileComponents();

    fixture = TestBed.createComponent(PrepareMigrationComponent);
    component = fixture.componentInstance;
    dialog = TestBed.inject(MatDialog);
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(PrepareMigrationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should open the dialog and set values after closed', () => {
    const mockValues: { [key: string]: string } = {
      [TargetDetails.TargetDB]: 'mockTargetDB',
      [TargetDetails.GcsMetadataName]: 'mockGcsMetadataName',
      [TargetDetails.GcsMetadataRootPath]: 'mockGcsMetadataRootPath',
      [TargetDetails.DefaultTimezone]: 'mockDefaultTimezone',
      [TargetDetails.DefaultTransactionIsolation]: 'repeatable read',
      [MigrationDetails.IsGcsMetadataPathSet]: 'true'
    };

    vi.spyOn(Storage.prototype, 'getItem').mockImplementation((key: string) => mockValues[key] ?? null);
    const dialogOpenSpy = vi.spyOn(dialog, 'open').mockReturnValue(dialogRefSpy as any);

    component.openGcsMetadataDetailsForm();

    expect(dialogOpenSpy).toHaveBeenCalledWith(GcsMetadataDetailsFormComponent, {
      width: '30vw',
      minWidth: '400px',
      maxWidth: '500px',
    });

    dialogRefSpy.afterClosed().subscribe(() => {
      expect(component.isGcsMetadataDetailSet).toBe(true);
      expect(component.targetDetails).toEqual({
        TargetDB: 'mockTargetDB',
        GcsMetadataPath: {
          GcsBucketName: 'mockGcsMetadataName',
          GcsBucketRootPath: 'mockGcsMetadataRootPath',
        },
        DefaultTimezone: 'mockDefaultTimezone',
        DefaultTransactionIsolation: 'repeatable read',
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
    fetchServiceSpy.getSourceDestinationSummary.mockReturnValue(of(mockSummary as any));

    component.ngOnInit();

    expect(component.migrationModes).toEqual([MigrationModes.schemaOnly]);
    expect(component.selectedMigrationMode).toEqual(MigrationModes.schemaOnly);
  });

  it('should seed DefaultTransactionIsolation from conv.DatabaseOptions when target details are not set', () => {
    vi.spyOn(Storage.prototype, 'getItem').mockReturnValue(null);
    const setItemSpy = vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {});
    component.isTargetDetailSet = false;
    (component as any).data.convSubject.next({
      DatabaseOptions: {
        DbName: 'db',
        DefaultTimezone: '',
        DefaultTransactionIsolation: 'repeatable read',
      },
    } as IConv);

    expect(setItemSpy).toHaveBeenCalledWith(TargetDetails.DefaultTransactionIsolation, 'repeatable read');
    expect(component.targetDetails.DefaultTransactionIsolation).toEqual('repeatable read');
  });

  it('should not overwrite cleared DefaultTransactionIsolation when target details are already set', () => {
    vi.spyOn(Storage.prototype, 'getItem').mockReturnValue(null);
    const setItemSpy = vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {});
    component.isTargetDetailSet = true;
    component.targetDetails.DefaultTransactionIsolation = '';
    (component as any).data.convSubject.next({
      DatabaseOptions: {
        DbName: 'db',
        DefaultTimezone: '',
        DefaultTransactionIsolation: 'repeatable read',
      },
    } as IConv);

    expect(setItemSpy).not.toHaveBeenCalledWith(TargetDetails.DefaultTransactionIsolation, 'repeatable read');
    expect(component.targetDetails.DefaultTransactionIsolation).toEqual('');
  });

  it('should not re-seed cleared DefaultTransactionIsolation after refreshPrerequisites when IsTargetDetailSet is in localStorage', () => {
    vi.spyOn(Storage.prototype, 'getItem').mockImplementation((key: string) =>
      key === MigrationDetails.IsTargetDetailSet ? 'true' : null
    );
    const setItemSpy = vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {});
    component.refreshPrerequisites();
    component.targetDetails.DefaultTransactionIsolation = '';
    (component as any).data.convSubject.next({
      DatabaseOptions: {
        DbName: 'db',
        DefaultTimezone: '',
        DefaultTransactionIsolation: 'repeatable read',
      },
    } as IConv);

    expect(setItemSpy).not.toHaveBeenCalledWith(TargetDetails.DefaultTransactionIsolation, 'repeatable read');
    expect(component.targetDetails.DefaultTransactionIsolation).toEqual('');
  });

  it('should preserve DefaultTransactionIsolation in clearLocalStorage for retries and unsubscribe on destroy', () => {
    const removeItemSpy = vi.spyOn(Storage.prototype, 'removeItem').mockImplementation(() => {});
    component.clearLocalStorage();
    expect(removeItemSpy).not.toHaveBeenCalledWith(TargetDetails.DefaultTransactionIsolation);

    component.ngOnDestroy();
    expect(component.convObj.closed).toBe(true);
  });

  it('should sync conv.DatabaseOptions and call updateDatabaseOptions when TargetDetails dialog closes', () => {
    const mockValues: { [key: string]: string } = {
      [TargetDetails.TargetDB]: 'orders-db',
      [TargetDetails.DefaultTimezone]: 'UTC',
      [TargetDetails.DefaultTransactionIsolation]: 'serializable',
      [MigrationDetails.IsTargetDetailSet]: 'true',
    };
    vi.spyOn(Storage.prototype, 'getItem').mockImplementation((key: string) => mockValues[key] ?? null);
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {});
    component.conv = {
      DatabaseOptions: {
        DbName: 'old-db',
        DefaultTimezone: '',
        DefaultTransactionIsolation: 'repeatable read',
      },
    } as IConv;

    component.openTargetDetailsForm();

    expect(component.conv.DatabaseOptions?.DbName).toEqual('orders-db');
    expect(component.conv.DatabaseOptions?.DefaultTimezone).toEqual('UTC');
    expect(component.conv.DatabaseOptions?.DefaultTransactionIsolation).toEqual('serializable');
    expect(fetchServiceSpy.updateDatabaseOptions).toHaveBeenCalledWith(
      expect.objectContaining({
        TargetDB: 'orders-db',
        DefaultTimezone: 'UTC',
        DefaultTransactionIsolation: 'serializable',
      })
    );
  });
});
