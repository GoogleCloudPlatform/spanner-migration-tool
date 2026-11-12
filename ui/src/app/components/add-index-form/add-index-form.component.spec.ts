import { provideHttpClient, withInterceptorsFromDi } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormArray, FormBuilder, ReactiveFormsModule } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { of } from 'rxjs';
import IConv from 'src/app/model/conv';
import IRule from 'src/app/model/rule';
import { ConversionService } from 'src/app/services/conversion/conversion.service';
import { DataService } from 'src/app/services/data/data.service';
import { SidenavService } from 'src/app/services/sidenav/sidenav.service';
import { createMockIConv } from 'src/mocks/conv';
import { AddIndexFormComponent } from './add-index-form.component';

describe('AddIndexFormComponent', () => {
  let component: AddIndexFormComponent;
  let fixture: ComponentFixture<AddIndexFormComponent>;
  let dataServiceSpy: any;
  let sidenavServiceSpy: any;
  let fb: FormBuilder;
  let conversionServiceSpy: any;
  let localIConv: IConv;

  beforeEach(async () => {
    localIConv = createMockIConv();
    dataServiceSpy = {
      conv: of(localIConv),
      applyRule: vi.fn(),
      dropRule: vi.fn(),
    };
    sidenavServiceSpy = {
      sidenavAddIndexTable: of("t1"),
      setSidenavAddIndexTable: vi.fn(),
      closeSidenav: vi.fn(),
      displayRuleFlag: of(true),
      ruleData: of(),
    };
    conversionServiceSpy = {
      getTableIdFromSpName: vi.fn().mockReturnValue("t1"),
      getColIdFromSpannerColName: vi.fn().mockReturnValue("TestId"),
    };

    await TestBed.configureTestingModule({
      declarations: [AddIndexFormComponent],
      imports: [ReactiveFormsModule, MatSnackBarModule, MatSelectModule, BrowserAnimationsModule, MatFormFieldModule, MatInputModule],
      providers: [
        FormBuilder,
        { provide: DataService, useValue: dataServiceSpy },
        { provide: SidenavService, useValue: sidenavServiceSpy },
        { provide: ConversionService, useValue: conversionServiceSpy },
        provideHttpClient(withInterceptorsFromDi()), provideHttpClientTesting(),
      ]
    })
      .compileComponents();

    fb = TestBed.inject(FormBuilder);
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AddIndexFormComponent);
    component = fixture.componentInstance;
    component.addIndexForm = fb.group({
      tableName: [''],
      indexName: ['', []],
      ColsArray: fb.array([]),
    });
    fixture.detectChanges();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should render ngOnInit and all dependent services correctly', () => {
    const addIndexRule: IRule = {
      Id: "r1",
      Name: "x1",
      ObjectType: "Table",
      Type: "add_index",
      Enabled: true,
      Data: {
        Id: "ind1",
        Name: "index1",
        TableId: "t1",
        Keys: [
          {
            ColId: "c1",
            Order: 1,
            Desc: false,
          }
        ]
      }
    }
    sidenavServiceSpy.ruleData = of(addIndexRule)
    component.ngOnInit()
    component.ngOnInit()
    const columnName = localIConv.SpSchema[addIndexRule.Data.TableId]?.ColDefs[addIndexRule.Data.Keys[0].ColId].Name;
    expect(component.ColsArray.length).toBe(1);
    expect(component.ColsArray.at(0).value.columnName).toEqual(columnName);
    expect(component.addIndexForm.controls['tableName'].value).toEqual(localIConv.SpSchema[addIndexRule.Data.TableId].Name);
    expect(component.addIndexForm.controls['indexName'].value).toEqual(addIndexRule.Data.Name)
    expect(component.addIndexForm.status).toEqual("DISABLED");
  });

  it('should add a new column form', () => {
    component.addNewColumnForm();
    expect(component.ColsArray.length).toBe(1);
  });

  it('should remove a column form', () => {
    component.addNewColumnForm();
    component.addNewColumnForm();
    component.removeColumnForm(1);
    expect(component.ColsArray.length).toBe(1);
  });

  it('should reset rule type', () => {
    vi.spyOn(component.resetRuleType, 'emit');
    component.resetRuleType.emit('');
    expect(component.resetRuleType.emit).toHaveBeenCalledWith('');
  });

  it('should set column arrays for view rules', () => {
    const tableId = 't1';
    const ruleData = {
      Keys: [
        {
          ColId: 'c1',
          Desc: false,
          Order: 1,
        },
      ],
    };
    component.setColArraysForViewRules(tableId, ruleData.Keys);
    const columnName = localIConv.SpSchema[tableId]?.ColDefs[ruleData.Keys[0].ColId].Name;
    expect(component.ColsArray.length).toBe(1);
    expect(component.ColsArray.at(0).value.columnName).toEqual(columnName);
  });

  it('should clear column arrays for empty data', () => {
    const tableId = 't1';
    component.setColArraysForViewRules(tableId, undefined);
    expect(component.ColsArray.length).toBe(0);
  });

  it('should update common columns', () => {
    component.totalColumns = ['Column1', 'Column2', 'Column3'];
    component.addIndexForm = fb.group({
      ColsArray: fb.array([
        fb.group({ columnName: 'Column1', sort: 'true' }),
        fb.group({ columnName: 'Column3', sort: 'false' }),
      ]),
    });
    component.updateCommonColumns();
    expect(component.commonColumns).toEqual(['Column2']);
  });

  it('should select table change', () => {
    const tableName = 'table1';
    component.selectedTableChange(tableName);
    expect(component.totalColumns).toEqual(['column1']);
    expect(component.ColsArray.length).toBe(0);
    expect(component.commonColumns).toEqual(['column1']);
    expect(component.addColumnsList).toEqual([]);
  });

  it('should delete a rule', () => {
    component.ruleId = 'sampleRuleId';
    component.deleteRule();
    expect(dataServiceSpy.dropRule).toHaveBeenCalledWith('sampleRuleId');
    expect(sidenavServiceSpy.setSidenavAddIndexTable).toHaveBeenCalledWith('');
    expect(sidenavServiceSpy.closeSidenav).toHaveBeenCalled();
  });

  it('should add an index and apply a rule', () => {
    component.addIndexForm = fb.group({
      tableName: 'table1',
      indexName: 'ind1',
      ColsArray: fb.array([
        fb.group({ columnName: 'column1', sort: 'true' }),
      ]),
    });
    const tableNameValue = component.addIndexForm.get('tableName')?.value;
    const colsArray = component.addIndexForm.get('ColsArray') as FormArray;
    const columnNameValue = colsArray.at(0)?.get('columnName')?.value;
    component.addIndex();
    expect(conversionServiceSpy.getTableIdFromSpName).toHaveBeenCalledWith(tableNameValue, component.conv);
    expect(conversionServiceSpy.getColIdFromSpannerColName).toHaveBeenCalledWith(columnNameValue, 't1', component.conv);
    expect(dataServiceSpy.applyRule).toHaveBeenCalled();
    expect(sidenavServiceSpy.setSidenavAddIndexTable).toHaveBeenCalledWith('');
    expect(sidenavServiceSpy.closeSidenav).toHaveBeenCalled();
  });

});
