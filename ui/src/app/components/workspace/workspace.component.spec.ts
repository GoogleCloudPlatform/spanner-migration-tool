import { ComponentFixture, TestBed } from '@angular/core/testing'
import { HttpClientModule } from '@angular/common/http'

import { WorkspaceComponent } from './workspace.component'
import { MatDialog, MatDialogModule } from '@angular/material/dialog'
import { MatSnackBarModule } from '@angular/material/snack-bar'
import { RouterTestingModule } from '@angular/router/testing'
import { MatMenuModule } from '@angular/material/menu'
import { ClickEventService } from 'src/app/services/click-event/click-event.service'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'
import { InputType, StorageKeys } from 'src/app/app.constants'
import { Observable, of } from 'rxjs'
import * as JSZip from 'jszip'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import mockIConv, { mockIConv2 } from 'src/mocks/conv'
import mockSpannerConfig from 'src/mocks/spannerConfig'
import { ConversionService } from 'src/app/services/conversion/conversion.service'
import { DataService } from 'src/app/services/data/data.service'
import { Router, RouterModule, Routes } from '@angular/router'
import IConv from 'src/app/model/conv'
import IStructuredReport from 'src/app/model/structured-report'
import ISpannerConfig from 'src/app/model/spanner-config'
const mockStructuredReport: IStructuredReport = {
  summary: {
    text: "",
    rating: "",
    dbName: "",
  },
  isSharded: false,
  ignoredStatements: [],
  conversionMetadata: [],
  migrationType: '',
  statementStats: {
    driverName: "",
    statementStats: []
  },
  nameChanges: [],
  tableReports: [],
  unexpectedConditions: {
    Reparsed: 0,
    unexpectedConditions: []
  },
  schemaOnly: false
};


describe('WorkspaceComponent', () => {
  let component: WorkspaceComponent
  let fixture: ComponentFixture<WorkspaceComponent>
  let dialogSpy: jasmine.SpyObj<MatDialog>;
  let clickEventSpy: jasmine.SpyObj<ClickEventService>;
  let sidenavSpy: jasmine.SpyObj<SidenavService>;
  let fetchServiceSpy: jasmine.SpyObj<FetchService>;
  let routerSpy: jasmine.SpyObj<Router>;

  beforeEach(async () => {
    const dialogSpyObj = jasmine.createSpyObj('MatDialog', ['open']);
    const clickEventSpyObj = jasmine.createSpyObj('ClickEventService', ['setViewAssesmentData']);
    const sidenavSpyObj = jasmine.createSpyObj('SidenavService', ['openSidenav', 'setSidenavComponent', 'setSidenavDatabaseName', 'setMiddleColumnComponent']);
    fetchServiceSpy = jasmine.createSpyObj('FetchService', ['getDStructuredReport', 'getDTextReport', 'getDSpannerDDL', 'getSpannerConfig', 'getIsOffline', 'getLastSessionDetails', 'getTableWithErrors']);
    const conversionServiceSpy = jasmine.createSpyObj('ConversionService', [
      'getStandardTypeToPGSQLTypemap',
      'getPGSQLToStandardTypeTypemap',
      'isIndexAddedOrRemoved',
      'getFkMapping',
      'getColumnMapping',
      'getIndexMapping',
    ]);
    const dataServiceSpy = jasmine.createSpyObj('DataService', [
      'getRateTypemapAndSummary',
    ]);
    routerSpy = jasmine.createSpyObj('Router', ['navigate']);

    await TestBed.configureTestingModule({
      declarations: [WorkspaceComponent],
      imports: [HttpClientModule, MatSnackBarModule, MatMenuModule],
      providers: [
        { provide: MatDialog, useValue: dialogSpyObj },
        { provide: ClickEventService, useValue: clickEventSpyObj },
        { provide: SidenavService, useValue: sidenavSpyObj },
        { provide: FetchService, useValue: fetchServiceSpy },
        { provide: ConversionService, useValue: conversionServiceSpy },
        { provide: DataService, useValue: dataServiceSpy },
        { provide: Router, useValue: routerSpy }
      ],
    }).compileComponents()

    dataServiceSpy.typeMap = of();
    dataServiceSpy.defaultTypeMap = of();
    dataServiceSpy.ddl = of();
    dataServiceSpy.conversionRate = of({
      t1: 'EXCELLENT',
      t2: 'GOOD',
      t3: 'OK',
      t4: 'BAD',
    });
    dataServiceSpy.isOffline = of(false);
    dataServiceSpy.conv = of(mockIConv);
    sidenavSpyObj.setMiddleColumnComponent = of(false);
    fetchServiceSpy.getLastSessionDetails.and.returnValue(of(mockIConv));
    fetchServiceSpy.getSpannerConfig.and.returnValue(of(mockSpannerConfig));
    fetchServiceSpy.getIsOffline.and.returnValue(of(false));
    fetchServiceSpy.getDStructuredReport.and.returnValue(of({} as any));
    fetchServiceSpy.getDTextReport.and.returnValue(of('textReport'));
    fetchServiceSpy.getDSpannerDDL.and.returnValue(of('spannerDDL'));
    conversionServiceSpy.isIndexAddedOrRemoved.and.returnValue(false);

    dialogSpy = TestBed.inject(MatDialog) as jasmine.SpyObj<MatDialog>;
    clickEventSpy = TestBed.inject(ClickEventService) as jasmine.SpyObj<ClickEventService>;
    sidenavSpy = TestBed.inject(SidenavService) as jasmine.SpyObj<SidenavService>;
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(WorkspaceComponent)
    component = fixture.componentInstance
    component.srcDbName = ''
    let store: { [key: string]: string } = {};
    const mockLocalStorage = {
      getItem: (key: string): string | null => {
        return key in store ? store[key] : null;
      },
      setItem: (key: string, value: string) => {
        store[key] = `${value}`;
      },
      removeItem: (key: string) => {
        delete store[key];
      },
      clear: () => {
        store = {};
      }
    };
    spyOn(localStorage, 'getItem')
      .and.callFake(mockLocalStorage.getItem);
    spyOn(localStorage, 'setItem')
      .and.callFake(mockLocalStorage.setItem);
    spyOn(localStorage, 'removeItem')
      .and.callFake(mockLocalStorage.removeItem);
    spyOn(localStorage, 'clear')
      .and.callFake(mockLocalStorage.clear);
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })

  it('should open the assessment sidenav and set view data', () => {

    localStorage.setItem(StorageKeys.Type, InputType.DirectConnect);

    const config = {
      hostName: 'example.com',
      port: '5432'
    };
    localStorage.setItem(StorageKeys.Config, JSON.stringify(config));

    component.openAssessment();

    expect(sidenavSpy.openSidenav).toHaveBeenCalled();
    expect(sidenavSpy.setSidenavComponent).toHaveBeenCalledWith('assessment');

    const expectedConnectionDetail = `${config.hostName} : ${config.port}`;
    const expectedViewAssesmentData = {
      srcDbType: component.srcDbName,
      connectionDetail: expectedConnectionDetail,
      conversionRates: component.conversionRateCount,
    };
    expect(clickEventSpy.setViewAssesmentData).toHaveBeenCalledWith(expectedViewAssesmentData);
  });

  it('should open the save session sidenav and set database name', () => {
    const dbName = 'TestDatabase';
    component.conv = { DatabaseName: dbName } as any;
    component.openSaveSessionSidenav();
    expect(sidenavSpy.openSidenav).toHaveBeenCalled();
    expect(sidenavSpy.setSidenavComponent).toHaveBeenCalledWith('saveSession');
    expect(sidenavSpy.setSidenavDatabaseName).toHaveBeenCalledWith(dbName);
  });

  it('should handle table errors', () => {
    fetchServiceSpy.getTableWithErrors.and.returnValue(
      of([{ Name: 'TableA', Id: 't1' }, { Name: 'TableB', Id: 't2' }])
    );

    component.prepareMigration();

    // Assert that the dialog should be opened with the error message
    expect(dialogSpy.open).toHaveBeenCalledWith(jasmine.any(Function), {
      data: {
        message: 'Please fix the errors for the following tables to move ahead: TableA, TableB',
        type: 'error',
        title: 'Error in Spanner Draft',
      },
      maxWidth: '500px',
    });

    // Ensure that the router should not be navigated
    expect(routerSpy.navigate).not.toHaveBeenCalledWith(['/prepare-migration']);
  });

  it('should handle offline status', () => {

    fetchServiceSpy.getTableWithErrors.and.returnValue(of([]));
    // Set isOfflineStatus to true
    component.isOfflineStatus = true;

    component.prepareMigration();

    // Assert that the dialog should be opened with the error message
    expect(dialogSpy.open).toHaveBeenCalledWith(jasmine.any(Function), {
      data: {
        message: 'Please configure spanner project id and instance id to proceed',
        type: 'error',
        title: 'Configure Spanner',
      },
      maxWidth: '500px',
    });

    // Ensure that the router should not be navigated
    expect(routerSpy.navigate).not.toHaveBeenCalledWith(['/prepare-migration']);
  });

  it('should handle empty SpSchema', () => {
    fetchServiceSpy.getTableWithErrors.and.returnValue(of([]));

    // Set isOfflineStatus to false
    component.isOfflineStatus = false;

    // Set the component's SpSchema to an empty object
    component.conv = { SpSchema: {} } as any;

    component.prepareMigration();

    // Assert that the dialog should be opened with the error message
    expect(dialogSpy.open).toHaveBeenCalledWith(jasmine.any(Function), {
      data: {
        message: 'Please restore some table(s) to proceed with the migration',
        type: 'error',
        title: 'All tables skipped',
      },
      maxWidth: '500px',
    });

    // Ensure that the router should not be navigated
    expect(routerSpy.navigate).not.toHaveBeenCalledWith(['/prepare-migration']);
  });

  it('should navigate to prepare-migration', () => {
    fetchServiceSpy.getTableWithErrors.and.returnValue(of([]));

    component.isOfflineStatus = false;

    component.conv = { SpSchema: { TableA: {}, TableB: {} } } as any;

    component.prepareMigration();

    // Ensure that the router should be navigated to '/prepare-migration'
    expect(routerSpy.navigate).toHaveBeenCalledWith(['/prepare-migration']);

    // Ensure that the dialog should not be opened
    expect(dialogSpy.open).not.toHaveBeenCalled();
  });

  it('should update conversion rate percentages correctly', () => {
    component.updateConversionRatePercentages();

    expect(component.conversionRateCount.good).toEqual(1);
    expect(component.conversionRateCount.ok).toEqual(2);
    expect(component.conversionRateCount.bad).toEqual(1);

    const tableCount = Object.keys(component.conversionRates).length;
    const expectedGoodPercentage = ((1 / tableCount) * 100).toFixed(2);
    const expectedOkPercentage = ((2 / tableCount) * 100).toFixed(2);
    const expectedBadPercentage = ((1 / tableCount) * 100).toFixed(2);

    expect(component.conversionRatePercentages.good).toEqual(Number(expectedGoodPercentage));
    expect(component.conversionRatePercentages.ok).toEqual(Number(expectedOkPercentage));
    expect(component.conversionRatePercentages.bad).toEqual(Number(expectedBadPercentage));
  });

  it('should return true when indexes are added or removed', () => {
    // Create a mock data object with different indexes
    let mockData = mockIConv2;
    // Call the method with the mock data
    const result = component.isIndexAddedOrRemoved(mockData);
    // Expect the result to be true, indicating that indexes were added or removed
    expect(result).toBeTruthy();
  });

  it('should return false when indexes remain the same', () => {

    // Create a mock data object with the same indexes
    const mockData = mockIConv;

    // Call the method with the mock data
    const result = component.isIndexAddedOrRemoved(mockData);

    // Expect the result to be false, indicating that indexes remain the same
    expect(result).toBeFalsy();
  });

  it('should trigger downloadStructuredReport', () => {
    const aClickSpy = jasmine.createSpy('aClickSpy');
    
    // Set up the fetch service spy to return a mock structured report
    fetchServiceSpy.getDStructuredReport.and.returnValue(of(mockStructuredReport));

    // Create a mock 'a' element
    const aElement = document.createElement('a');
    spyOn(document, 'createElement').and.returnValue(aElement);

    // Spy on 'a.click()' method
    spyOn(aElement, 'click').and.callFake(() => {
      aClickSpy();
    });

    component.downloadStructuredReport();

    expect(fetchServiceSpy.getDStructuredReport).toHaveBeenCalled();
    expect(aClickSpy).toHaveBeenCalled();
  });

  it('should trigger downloadTextReport', () => {
    const aClickSpy = jasmine.createSpy('aClickSpy');

    fetchServiceSpy.getDTextReport.and.returnValue(of("text"));

    const aElement = document.createElement('a');
    spyOn(document, 'createElement').and.returnValue(aElement);

    spyOn(aElement, 'click').and.callFake(() => {
      aClickSpy();
    });

    component.downloadTextReport();

    expect(fetchServiceSpy.getDTextReport).toHaveBeenCalled();
    expect(aClickSpy).toHaveBeenCalled();
  });

  it('should trigger downloadDdl', () => {
    const aClickSpy = jasmine.createSpy('aClickSpy');

    fetchServiceSpy.getDSpannerDDL.and.returnValue(of("ddl"));

    const aElement = document.createElement('a');
    spyOn(document, 'createElement').and.returnValue(aElement);

    spyOn(aElement, 'click').and.callFake(() => {
      aClickSpy();
    });

    component.downloadDDL();

    expect(fetchServiceSpy.getDSpannerDDL).toHaveBeenCalled();
    expect(aClickSpy).toHaveBeenCalled();
  });



  /*it('should trigger downloadArtifacts', async () => {
    const aClickSpy = jasmine.createSpy('aClickSpy');
    const generateAsyncSpy = spyOn(JSZip.prototype, 'generateAsync').and.returnValue(Promise.resolve({} as any));
    const mockTextReport = 'Mock text report';
    const mockSpannerDDL = 'Mock spanner DDL';
    const mockSpannerConfig: ISpannerConfig = {
      GCPProjectID: "",
      SpannerInstanceID: ""
    }

    // Set up the fetch service spies to return mock data
    fetchServiceSpy.getDStructuredReport.and.returnValue(of(mockStructuredReport));
    fetchServiceSpy.getDTextReport.and.returnValue(of(mockTextReport));
    fetchServiceSpy.getDSpannerDDL.and.returnValue(of(mockSpannerDDL));
    fetchServiceSpy.getSpannerConfig.and.returnValue(of(mockSpannerConfig));

    // Create a mock 'a' element
    const aElement = document.createElement('a');
    spyOn(document, 'createElement').and.returnValue(aElement);

    // Spy on 'a.click()' method
    spyOn(aElement, 'click').and.callFake(() => {
      aClickSpy();
    });

    // Call the method to test
    component.downloadArtifacts();

    await fixture.whenStable();

    // Expectations
    expect(fetchServiceSpy.getDStructuredReport).toHaveBeenCalled();
    expect(fetchServiceSpy.getDTextReport).toHaveBeenCalled();
    expect(fetchServiceSpy.getDSpannerDDL).toHaveBeenCalled();
    expect(generateAsyncSpy).toHaveBeenCalledOnceWith({ type: 'blob' });
    expect(aClickSpy).toHaveBeenCalled();
  });*/
})
