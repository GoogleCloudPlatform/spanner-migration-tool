import { ComponentFixture, fakeAsync, TestBed, tick } from '@angular/core/testing'
import { HttpClientModule } from '@angular/common/http'

import { WorkspaceComponent } from './workspace.component'
import { MatDialog } from '@angular/material/dialog'
import { MatSnackBarModule } from '@angular/material/snack-bar'
import { MatMenuModule } from '@angular/material/menu'
import { ClickEventService } from 'src/app/services/click-event/click-event.service'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'
import { InputType, ObjectExplorerNodeType, SourceDbNames, StorageKeys } from 'src/app/app.constants'
import { of, Subscription } from 'rxjs'
import * as JSZip from 'jszip'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import mockIConv, { mockIConv2 } from 'src/mocks/conv'
import mockSpannerConfig from 'src/mocks/spannerConfig'
import { ConversionService } from 'src/app/services/conversion/conversion.service'
import { DataService } from 'src/app/services/data/data.service'
import { Router } from '@angular/router'
import IStructuredReport from 'src/app/model/structured-report'
import ISpannerConfig from 'src/app/model/spanner-config'
import { MatTabsModule } from '@angular/material/tabs'
import { BrowserAnimationsModule } from '@angular/platform-browser/animations'
import { FlatNode } from 'src/app/model/schema-object-node'
import IConv from 'src/app/model/conv'
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
  let dialogSpyObj: jasmine.SpyObj<MatDialog>;
  let clickEventSpyObj: jasmine.SpyObj<ClickEventService>;
  let sidenavSpyObj: jasmine.SpyObj<SidenavService>;
  let fetchServiceSpy: jasmine.SpyObj<FetchService>;
  let dataServiceSpy: jasmine.SpyObj<DataService>;
  let routerSpy: jasmine.SpyObj<Router>;
  let conversionServiceSpy: jasmine.SpyObj<ConversionService>;

  beforeEach(async () => {
    dialogSpyObj = jasmine.createSpyObj('MatDialog', ['open']);
    clickEventSpyObj = jasmine.createSpyObj('ClickEventService', ['setViewAssesmentData', 'setTabToSpanner']);
    sidenavSpyObj = jasmine.createSpyObj('SidenavService', ['openSidenav', 'setSidenavComponent', 'setSidenavDatabaseName', 'setMiddleColumnComponent']);
    fetchServiceSpy = jasmine.createSpyObj('FetchService', ['getDStructuredReport', 'getDTextReport', 'getDSpannerDDL', 'getSpannerConfig', 'getIsOffline', 'getLastSessionDetails', 'getTableWithErrors']);
    conversionServiceSpy = jasmine.createSpyObj('ConversionService', [
      'getStandardTypeToPGSQLTypemap',
      'getPGSQLToStandardTypeTypemap',
      'isIndexAddedOrRemoved',
      'getFkMapping',
      'getColumnMapping',
      'getIndexMapping',
      'createTreeNode',
      'createTreeNodeForSource'
    ]);
    dataServiceSpy = jasmine.createSpyObj('DataService', [
      'getRateTypemapAndSummary',
    ]);
    routerSpy = jasmine.createSpyObj('Router', ['navigate']);

    await TestBed.configureTestingModule({
      declarations: [WorkspaceComponent],
      imports: [HttpClientModule, MatSnackBarModule, MatMenuModule, MatTabsModule, BrowserAnimationsModule],
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

    dataServiceSpy.typeMap = of({});
    dataServiceSpy.defaultTypeMap = of({});
    dataServiceSpy.ddl = of("");
    dataServiceSpy.conversionRate = of({
      t1: 'EXCELLENT',
      t2: 'GOOD',
      t3: 'OK',
      t4: 'BAD',
    });
    const mockData = {
      rates: {},
      typeMap: {},
      defaultTypeMap: {},
      summary: {},
      ddl: '',
    };
    const mockSubscription = new Subscription();
    mockSubscription.add(of(mockData).subscribe());
    dataServiceSpy.isOffline = of(false);
    dataServiceSpy.conv = of(mockIConv);
    dataServiceSpy.getRateTypemapAndSummary.and.returnValue(mockSubscription);
    sidenavSpyObj.setMiddleColumnComponent = of(false);
    fetchServiceSpy.getLastSessionDetails.and.returnValue(of(mockIConv));
    fetchServiceSpy.getSpannerConfig.and.returnValue(of(mockSpannerConfig));
    fetchServiceSpy.getIsOffline.and.returnValue(of(false));
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

  it('should call service methods and update properties on ngOnInit', () => {
    expect(conversionServiceSpy.getStandardTypeToPGSQLTypemap).toHaveBeenCalled();
    expect(conversionServiceSpy.getPGSQLToStandardTypeTypemap).toHaveBeenCalled();
    expect(dataServiceSpy.getRateTypemapAndSummary).toHaveBeenCalled();
    expect(component.typeMap).toEqual({});
    expect(component.defaultTypeMap).toEqual({});
    expect(component.ddlStmts).toEqual('');
    expect(component.dialect).toEqual('Google Standard SQL');
    expect(component.isMiddleColumnCollapse).toBeTruthy();
    expect(component.srcDbName).toEqual(SourceDbNames.MySQL);
    expect(component.objectExplorerInitiallyRender).toBeTruthy();
    mockIConv.SpDialect = "postgresql"
    component.ngOnInit();
    expect(component.dialect).toEqual('PostgreSQL')
  })

  it('should navigate to home page in case of empty source schema', () => {
    mockIConv.SrcSchema = {};
    component.ngOnInit();
    expect(routerSpy.navigate).toHaveBeenCalledWith(['/'])
  })

  it('existing conv and data mismatch', () => {
    component.conv = mockIConv2
    component.ngOnInit();
    expect(component.conv).toEqual(mockIConv)
  })

  it('should toggle left column', () => {
    component.isLeftColumnCollapse = false;
    component.leftColumnToggle();
    expect(component.isLeftColumnCollapse).toEqual(true);
  })

  it('should toggle right column', () => {
    component.isRightColumnCollapse = false;
    component.rightColumnToggle();
    expect(component.isRightColumnCollapse).toEqual(true);
  })

  it('should toggle middle column', () => {
    component.isMiddleColumnCollapse = false;
    component.middleColumnToggle();
    expect(component.isMiddleColumnCollapse).toEqual(true);
  })

  it('should update issues label', fakeAsync(() => {
    const count = 5;
    component.updateIssuesLabel(count);
    tick();
    fixture.detectChanges();
    const issuesLabelElement = fixture.nativeElement.querySelector('.mdc-tab__text-label');
    expect(issuesLabelElement).toBeTruthy();
    expect(issuesLabelElement.textContent).toContain(`ISSUES AND SUGGESTIONS (${count})`);
  }));

  it('should update rules label', fakeAsync(() => {
    const count = 3;
    component.updateRulesLabel(count);
    tick();
    fixture.detectChanges();
    const rulesLabelElement = fixture.nativeElement.querySelectorAll('.mdc-tab__text-label')[1];
    expect(rulesLabelElement.textContent).toContain(`RULES (${count})`);
  }));

  it('should open the assessment sidenav and set view data for direct connection', () => {
    localStorage.setItem(StorageKeys.Type, InputType.DirectConnect);
    const config = {
      hostName: 'localhost',
      port: '4600'
    };
    localStorage.setItem(StorageKeys.Config, JSON.stringify(config));
    component.openAssessment();
    expect(sidenavSpyObj.openSidenav).toHaveBeenCalled();
    expect(sidenavSpyObj.setSidenavComponent).toHaveBeenCalledWith('assessment');

    const expectedConnectionDetail = `${config.hostName} : ${config.port}`;
    const expectedViewAssesmentData = {
      srcDbType: component.srcDbName,
      connectionDetail: expectedConnectionDetail,
      conversionRates: component.conversionRateCount,
    };
    expect(clickEventSpyObj.setViewAssesmentData).toHaveBeenCalledWith(expectedViewAssesmentData);
  });

  it('empty hostname and port for direct connection', () => {

    localStorage.setItem(StorageKeys.Type, InputType.DirectConnect);

    component.openAssessment();

    expect(sidenavSpyObj.openSidenav).toHaveBeenCalled();
    expect(sidenavSpyObj.setSidenavComponent).toHaveBeenCalledWith('assessment');

    const expectedConnectionDetail = `undefined : undefined`;
    const expectedViewAssesmentData = {
      srcDbType: component.srcDbName,
      connectionDetail: expectedConnectionDetail,
      conversionRates: component.conversionRateCount,
    };
    expect(clickEventSpyObj.setViewAssesmentData).toHaveBeenCalledWith(expectedViewAssesmentData);
  });

  it('should open the assessment sidenav and set view data for dump file', () => {

    localStorage.setItem(StorageKeys.Type, InputType.DumpFile);
    component.openAssessment();
    expect(sidenavSpyObj.openSidenav).toHaveBeenCalled();
    expect(sidenavSpyObj.setSidenavComponent).toHaveBeenCalledWith('assessment');

    const expectedViewAssesmentData = {
      srcDbType: component.srcDbName,
      connectionDetail: component.conv.DatabaseName,
      conversionRates: component.conversionRateCount,
    };
    expect(clickEventSpyObj.setViewAssesmentData).toHaveBeenCalledWith(expectedViewAssesmentData);
  });

  it('should open the save session sidenav and set database name', () => {
    const dbName = 'TestDatabase';
    component.conv = { DatabaseName: dbName } as any;
    component.openSaveSessionSidenav();
    expect(sidenavSpyObj.openSidenav).toHaveBeenCalled();
    expect(sidenavSpyObj.setSidenavComponent).toHaveBeenCalledWith('saveSession');
    expect(sidenavSpyObj.setSidenavDatabaseName).toHaveBeenCalledWith(dbName);
  });

  it('should handle table errors', () => {
    fetchServiceSpy.getTableWithErrors.and.returnValue(
      of([{ Name: 'TableA', Id: 't1' }, { Name: 'TableB', Id: 't2' }])
    );
    component.prepareMigration();
    expect(dialogSpyObj.open).toHaveBeenCalledWith(jasmine.any(Function), {
      data: {
        message: 'Please fix the errors for the following tables to move ahead: TableA, TableB',
        type: 'error',
        title: 'Error in Spanner Draft',
      },
      maxWidth: '500px',
    });
    expect(routerSpy.navigate).not.toHaveBeenCalledWith(['/prepare-migration']);
  });

  it('should handle offline status', () => {

    fetchServiceSpy.getTableWithErrors.and.returnValue(of([]));
    component.isOfflineStatus = true;
    component.prepareMigration();
    expect(dialogSpyObj.open).toHaveBeenCalledWith(jasmine.any(Function), {
      data: {
        message: 'Please configure spanner project id and instance id to proceed',
        type: 'error',
        title: 'Configure Spanner',
      },
      maxWidth: '500px',
    });
    expect(routerSpy.navigate).not.toHaveBeenCalledWith(['/prepare-migration']);
  });

  it('should handle empty SpSchema', () => {
    fetchServiceSpy.getTableWithErrors.and.returnValue(of([]));
    component.isOfflineStatus = false;
    component.conv = { SpSchema: {} } as any;
    component.prepareMigration();
    expect(dialogSpyObj.open).toHaveBeenCalledWith(jasmine.any(Function), {
      data: {
        message: 'Please restore some table(s) to proceed with the migration',
        type: 'error',
        title: 'All tables skipped',
      },
      maxWidth: '500px',
    });
    expect(routerSpy.navigate).not.toHaveBeenCalledWith(['/prepare-migration']);
  });

  it('should navigate to prepare-migration', () => {
    fetchServiceSpy.getTableWithErrors.and.returnValue(of([]));
    component.isOfflineStatus = false;
    component.conv = { SpSchema: { TableA: {}, TableB: {} } } as any;
    component.prepareMigration();
    expect(routerSpy.navigate).toHaveBeenCalledWith(['/prepare-migration']);
    expect(dialogSpyObj.open).not.toHaveBeenCalled();
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
    let mockData = mockIConv2;
    const result = component.isIndexAddedOrRemoved(mockData);
    expect(result).toBeTruthy();
  });

  it('should return false when indexes remain the same', () => {
    const mockData = mockIConv;
    const result = component.isIndexAddedOrRemoved(mockData);
    expect(result).toBeFalsy();
  });

  it('should trigger downloadStructuredReport', () => {
    const aClickSpy = jasmine.createSpy('aClickSpy');
    fetchServiceSpy.getDStructuredReport.and.returnValue(of(mockStructuredReport));
    const aElement = document.createElement('a');
    spyOn(document, 'createElement').and.returnValue(aElement);
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

  it('should set spanner tab', () => {
    component.spannerTab()
    expect(clickEventSpyObj.setTabToSpanner).toHaveBeenCalled();
  })

  it('should set currentObject and tableData when type is Table', () => {
    const tableNode: FlatNode = {
      id: 'table1',
      type: ObjectExplorerNodeType.Table,
      expandable: false,
      name: '',
      status: undefined,
      pos: 0,
      level: 0,
      isSpannerNode: false,
      isDeleted: false,
      parent: '',
      parentId: ''
    };
    conversionServiceSpy.getColumnMapping.withArgs(jasmine.any(String), jasmine.objectContaining<IConv>({})).and.returnValue([]);
    conversionServiceSpy.getFkMapping.withArgs(jasmine.any(String), jasmine.objectContaining<IConv>({})).and.returnValue([]);

    component.changeCurrentObject(tableNode);

    expect(component.currentObject).toEqual(tableNode);
    expect(component.tableData).toEqual([]);
    expect(component.fkData).toEqual([]);
  });

  it('should set currentObject and indexData when type is Index', () => {
    const indexNode: FlatNode = {
      id: 'index1',
      type: ObjectExplorerNodeType.Index,
      parentId: 'table1',
      expandable: false,
      name: '',
      status: undefined,
      pos: 0,
      level: 0,
      isSpannerNode: false,
      isDeleted: false,
      parent: ''
    };
    conversionServiceSpy.getIndexMapping.withArgs(jasmine.any(String),jasmine.objectContaining<IConv>({}),jasmine.any(String)).and.returnValue([]);
    component.changeCurrentObject(indexNode);
    expect(component.currentObject).toEqual(indexNode);
    expect(component.indexData).toEqual([]);
  });

  it('should set currentObject to null when type is neither Table nor Index', () => {
    const unsupportedNode: FlatNode = {
      id: 'id1',
      type: ObjectExplorerNodeType.Indexes,
      expandable: false,
      name: '',
      status: undefined,
      pos: 0,
      level: 0,
      isSpannerNode: false,
      isDeleted: false,
      parent: '',
      parentId: ''
    };
    component.changeCurrentObject(unsupportedNode);
    expect(component.currentObject).toBeNull();
  });
})
