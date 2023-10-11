import { HttpClientModule } from '@angular/common/http';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormBuilder, ReactiveFormsModule } from '@angular/forms';
import { MatFormFieldModule } from '@angular/material/form-field';
import { MatInputModule } from '@angular/material/input';
import { MatSelectModule } from '@angular/material/select';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';
import { of } from 'rxjs';
import IRule from 'src/app/model/rule';
import { ConversionService } from 'src/app/services/conversion/conversion.service';
import { DataService } from 'src/app/services/data/data.service';
import { SidenavService } from 'src/app/services/sidenav/sidenav.service';
import mockIConv from 'src/mocks/conv';

import { AddIndexFormComponent } from './add-index-form.component';

describe('AddIndexFormComponent', () => {
  let component: AddIndexFormComponent;
  let fixture: ComponentFixture<AddIndexFormComponent>;
  let dataServiceSpy: jasmine.SpyObj<DataService>;
  let sidenavServiceSpy: jasmine.SpyObj<SidenavService>;
  let fb: FormBuilder;
  let conversionServiceSpy: jasmine.SpyObj<ConversionService>;

  beforeEach(async () => {
    dataServiceSpy = jasmine.createSpyObj('DataService', ['conv', 'applyRule', 'dropRule']);
    sidenavServiceSpy = jasmine.createSpyObj('SidenavService', [
      'sidenavAddIndexTable',
      'setSidenavAddIndexTable',
      'closeSidenav',
      'displayRuleFlag',
      'ruleData',
    ]);
    conversionServiceSpy = jasmine.createSpyObj('ConversionService', [
      'getTableIdFromSpName',
      'getColIdFromSpannerColName',
    ]);

    
    await TestBed.configureTestingModule({
      declarations: [AddIndexFormComponent],
      imports: [ReactiveFormsModule, HttpClientModule, MatSnackBarModule, MatSelectModule, BrowserAnimationsModule, MatFormFieldModule, MatInputModule],
      providers: [
        FormBuilder,
        { provide: DataService, useValue: dataServiceSpy },
        { provide: SidenavService, useValue: sidenavServiceSpy },
        { provide: ConversionService, useValue: conversionServiceSpy },
      ],
    })
      .compileComponents();

    fb = TestBed.inject(FormBuilder);

    conversionServiceSpy.getColIdFromSpannerColName.and.returnValue("TestId")
    conversionServiceSpy.getTableIdFromSpName.and.returnValue("t1")
    dataServiceSpy.conv = of(mockIConv);
    dataServiceSpy.dropRule.and.callThrough();
    sidenavServiceSpy.sidenavAddIndexTable = of("t1");
    sidenavServiceSpy.displayRuleFlag = of(true)
    sidenavServiceSpy.ruleData = of()
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
        Name: "ind1",
        Table: "t1",
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
    expect(component.ColsArray.length).toBe(1);
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
    spyOn(component.resetRuleType, 'emit');
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
    expect(component.ColsArray.length).toBe(1);
    expect(component.ColsArray.at(0).value.columnName).toEqual('column1');
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
    // Set a mock ruleId
    component.ruleId = 'sampleRuleId';
    // Call the deleteRule method
    component.deleteRule();
    // Expectations
    expect(dataServiceSpy.dropRule).toHaveBeenCalledWith('sampleRuleId');
    expect(sidenavServiceSpy.setSidenavAddIndexTable).toHaveBeenCalledWith('');
    expect(sidenavServiceSpy.closeSidenav).toHaveBeenCalled();
  });

  it('should add an index and apply a rule', () => {
    // Mock form values
    component.addIndexForm = fb.group({
      tableName: 'table1',
      indexName: 'ind1',
      ColsArray: fb.array([
        fb.group({ columnName: 'column1', sort: 'true' }),
      ]),
    });
    component.addIndex();
    expect(conversionServiceSpy.getTableIdFromSpName).toHaveBeenCalledWith('table1', component.conv);
    expect(conversionServiceSpy.getColIdFromSpannerColName).toHaveBeenCalledWith('column1', 't1', component.conv);
    expect(dataServiceSpy.applyRule).toHaveBeenCalled();
    expect(sidenavServiceSpy.setSidenavAddIndexTable).toHaveBeenCalledWith('');
    expect(sidenavServiceSpy.closeSidenav).toHaveBeenCalled();
  });

});
