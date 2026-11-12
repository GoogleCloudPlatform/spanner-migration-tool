import { NO_ERRORS_SCHEMA } from '@angular/core'
import { ComponentFixture, TestBed } from '@angular/core/testing'
import { provideHttpClient, withInterceptorsFromDi } from '@angular/common/http'
import { provideHttpClientTesting } from '@angular/common/http/testing';

import { WorkspaceComponent } from './workspace.component'
import { MatDialog } from '@angular/material/dialog'
import { MatSnackBarModule } from '@angular/material/snack-bar'
import { MatMenuModule } from '@angular/material/menu'
import { ClickEventService } from 'src/app/services/click-event/click-event.service'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'
import { InputType, ObjectExplorerNodeType, SourceDbNames, StorageKeys } from 'src/app/app.constants'
import { of, Subscription } from 'rxjs'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { createMockIConv, createMockIConv2 } from 'src/mocks/conv'
import mockSpannerConfig from 'src/mocks/spannerConfig'
import { ConversionService } from 'src/app/services/conversion/conversion.service'
import { DataService } from 'src/app/services/data/data.service'
import { Router } from '@angular/router'
import IStructuredReport from 'src/app/model/structured-report'
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
  let mockIConv: IConv;
  let dialogSpyObj: any;
  let clickEventSpyObj: any;
  let sidenavSpyObj: any;
  let fetchServiceSpy: any;
  let dataServiceSpy: any;
  let routerSpy: any;
  let conversionServiceSpy: any;

  beforeEach(async () => {
    mockIConv = createMockIConv();
    const mockData = {
      rates: {},
      typeMap: {},
      defaultTypeMap: {},
      summary: {},
      ddl: '',
    };
    const mockSubscription = new Subscription();
    mockSubscription.add(of(mockData).subscribe());

    dialogSpyObj = { open: vi.fn() };
    clickEventSpyObj = {
      setViewAssesmentData: vi.fn(),
      setTabToSpanner: vi.fn(),
    };
    sidenavSpyObj = {
      openSidenav: vi.fn(),
      setSidenavComponent: vi.fn(),
      setSidenavDatabaseName: vi.fn(),
      setMiddleColumnComponent: of(false),
    };
    fetchServiceSpy = {
      getDStructuredReport: vi.fn(),
      getDTextReport: vi.fn(),
      getDSpannerDDL: vi.fn(),
      getSpannerConfig: vi.fn().mockReturnValue(of(mockSpannerConfig)),
      getIsOffline: vi.fn().mockReturnValue(of(false)),
      getLastSessionDetails: vi.fn().mockReturnValue(of(mockIConv)),
      getTableWithErrors: vi.fn(),
      verifyCheckConstraintExpression: vi.fn(),
    };
    conversionServiceSpy = {
      getStandardTypeToPGSQLTypemap: vi.fn(),
      getPGSQLToStandardTypeTypemap: vi.fn(),
      isIndexAddedOrRemoved: vi.fn(),
      getFkMapping: vi.fn(),
      getColumnMapping: vi.fn(),
      getIndexMapping: vi.fn(),
      createTreeNode: vi.fn(),
      createTreeNodeForSource: vi.fn(),
      getCheckConstraints: vi.fn(),
    };
    dataServiceSpy = {
      getRateTypemapAndSummary: vi.fn().mockReturnValue(mockSubscription),
      verifyCheckConstraintExpression: vi.fn(),
      typeMap: of({}),
      defaultTypeMap: of({}),
      ddl: of(""),
      conversionRate: of({
        t1: 'EXCELLENT',
        t2: 'GOOD',
        t3: 'OK',
        t4: 'BAD',
      }),
      isOffline: of(false),
      conv: of(mockIConv),
    };
    routerSpy = { navigate: vi.fn() };

    await TestBed.configureTestingModule({
      schemas: [NO_ERRORS_SCHEMA],
      declarations: [WorkspaceComponent],
      imports: [MatSnackBarModule, MatMenuModule, MatTabsModule, BrowserAnimationsModule],
      providers: [
        { provide: MatDialog, useValue: dialogSpyObj },
        { provide: ClickEventService, useValue: clickEventSpyObj },
        { provide: SidenavService, useValue: sidenavSpyObj },
        { provide: FetchService, useValue: fetchServiceSpy },
        { provide: ConversionService, useValue: conversionServiceSpy },
        { provide: DataService, useValue: dataServiceSpy },
        { provide: Router, useValue: routerSpy },
        provideHttpClient(withInterceptorsFromDi()), provideHttpClientTesting()
      ]
    }).compileComponents()
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
    vi.spyOn(Storage.prototype, 'getItem').mockImplementation(mockLocalStorage.getItem);
    vi.spyOn(Storage.prototype, 'setItem').mockImplementation(mockLocalStorage.setItem);
    vi.spyOn(Storage.prototype, 'removeItem').mockImplementation(mockLocalStorage.removeItem);
    vi.spyOn(Storage.prototype, 'clear').mockImplementation(mockLocalStorage.clear);
    fixture.detectChanges()
  })

  afterEach(() => {
    vi.restoreAllMocks();
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
    component.conv = createMockIConv2()
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

  it('should update issues label', async () => {
    const count = 5;
    component.updateIssuesLabel(count);
    await new Promise((resolve) => setTimeout(resolve, 0));
    fixture.detectChanges();
    const issuesLabelElement = fixture.nativeElement.querySelector('.mdc-tab__text-label');
    expect(issuesLabelElement).toBeTruthy();
    expect(issuesLabelElement.textContent).toContain(`ISSUES AND SUGGESTIONS (${count})`);
  });

  it('should update rules label', async () => {
    const count = 3;
    component.updateRulesLabel(count);
    await new Promise((resolve) => setTimeout(resolve, 0));
    fixture.detectChanges();
    const rulesLabelElement = fixture.nativeElement.querySelectorAll('.mdc-tab__text-label')[1];
    expect(rulesLabelElement.textContent).toContain(`RULES (${count})`);
  });

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
    fetchServiceSpy.getTableWithErrors.mockReturnValue(
      of([{ Name: 'TableA', Id: 't1' }, { Name: 'TableB', Id: 't2' }])
    );
    component.prepareMigration();
    expect(dialogSpyObj.open).toHaveBeenCalledWith(expect.any(Function), {
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

    fetchServiceSpy.getTableWithErrors.mockReturnValue(of([]));
    component.isOfflineStatus = true;
    component.prepareMigration();
    expect(dialogSpyObj.open).toHaveBeenCalledWith(expect.any(Function), {
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
    fetchServiceSpy.getTableWithErrors.mockReturnValue(of([]));
    component.isOfflineStatus = false;
    component.conv = { SpSchema: {} } as any;
    component.prepareMigration();
    expect(dialogSpyObj.open).toHaveBeenCalledWith(expect.any(Function), {
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
    fetchServiceSpy.getTableWithErrors.mockReturnValue(of([]));
    fetchServiceSpy.verifyCheckConstraintExpression.mockReturnValue(of(false))
    const mockObservable = of(false);
    dataServiceSpy.verifyCheckConstraintExpression.mockReturnValue(mockObservable)
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
    let mockData = createMockIConv2();
    const result = component.isIndexAddedOrRemoved(mockData);
    expect(result).toBeTruthy();
  });

  it('should return false when indexes remain the same', () => {
    const mockData = mockIConv;
    const result = component.isIndexAddedOrRemoved(mockData);
    expect(result).toBeFalsy();
  });

  it('should trigger downloadStructuredReport', () => {
    const aClickSpy = vi.fn();
    fetchServiceSpy.getDStructuredReport.mockReturnValue(of(mockStructuredReport));
    const aElement = document.createElement('a');
    vi.spyOn(document, 'createElement').mockReturnValue(aElement);
    vi.spyOn(aElement, 'click').mockImplementation(() => {
      aClickSpy();
    });
    component.downloadStructuredReport();
    expect(fetchServiceSpy.getDStructuredReport).toHaveBeenCalled();
    expect(aClickSpy).toHaveBeenCalled();
  });

  it('should trigger downloadTextReport', () => {
    const aClickSpy = vi.fn();
    fetchServiceSpy.getDTextReport.mockReturnValue(of("text"));
    const aElement = document.createElement('a');
    vi.spyOn(document, 'createElement').mockReturnValue(aElement);
    vi.spyOn(aElement, 'click').mockImplementation(() => {
      aClickSpy();
    });
    component.downloadTextReport();
    expect(fetchServiceSpy.getDTextReport).toHaveBeenCalled();
    expect(aClickSpy).toHaveBeenCalled();
  });

  it('should trigger downloadDdl', () => {
    const aClickSpy = vi.fn();
    fetchServiceSpy.getDSpannerDDL.mockReturnValue(of("ddl"));
    const aElement = document.createElement('a');
    vi.spyOn(document, 'createElement').mockReturnValue(aElement);
    vi.spyOn(aElement, 'click').mockImplementation(() => {
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
    conversionServiceSpy.getColumnMapping.mockReturnValue([]);
    conversionServiceSpy.getFkMapping.mockReturnValue([]);

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
    conversionServiceSpy.getCheckConstraints.mockReturnValue([])
    conversionServiceSpy.getIndexMapping.mockReturnValue([]);
    component.changeCurrentObject(indexNode);
    expect(component.currentObject).toEqual(indexNode);
    expect(component.indexData).toEqual([]);
    expect(component.ccData).toEqual([])
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
