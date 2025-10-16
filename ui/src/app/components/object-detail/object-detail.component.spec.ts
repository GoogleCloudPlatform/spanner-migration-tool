import { provideHttpClient, withInterceptorsFromDi } from '@angular/common/http'
import { ComponentFixture, TestBed } from '@angular/core/testing'
import { MatDialog, MatDialogModule } from '@angular/material/dialog'
import { By } from '@angular/platform-browser'
import IColumnTabData from 'src/app/model/edit-table'
import { ObjectDetailComponent } from './object-detail.component'
import { MatTableModule } from '@angular/material/table'
import { MatTabsModule } from '@angular/material/tabs'
import { BrowserAnimationsModule } from '@angular/platform-browser/animations'
import { MatDividerModule } from '@angular/material/divider'
import { MatIconModule } from '@angular/material/icon'
import { MatSnackBar} from '@angular/material/snack-bar'
import { DataService } from 'src/app/services/data/data.service'
import { ConversionService } from 'src/app/services/conversion/conversion.service'
import mockIConv from 'src/mocks/conv'
import { of } from 'rxjs'
import { FormBuilder } from '@angular/forms'
import { FlatNode } from 'src/app/model/schema-object-node'
import { DropObjectDetailDialogComponent } from '../drop-object-detail-dialog/drop-object-detail-dialog.component'
import { Dialect, ObjectDetailNodeType, SourceDbNames } from 'src/app/app.constants'
import { InfodialogComponent } from '../infodialog/infodialog.component'
import IUpdateTable from 'src/app/model/update-table'

describe('ObjectDetailComponent', () => {
  let component: ObjectDetailComponent
  let fixture: ComponentFixture<ObjectDetailComponent>
  let dataServiceSpy: jasmine.SpyObj<DataService>;
  let conversionServiceSpy: jasmine.SpyObj<ConversionService>
  let dialogSpyObj: jasmine.SpyObj<MatDialog>;
  let rowData: IColumnTabData[]

  beforeEach(async () => {    dataServiceSpy = jasmine.createSpyObj('DataService', ['updateSequence', 'dropSequence', 'updateCheckConstraint', 'reviewTableUpdate', 'setInterleave', 'dropTable', 'getConversionRate']);
    dataServiceSpy.updateSequence.and.returnValue(of({}));
    dataServiceSpy.dropSequence.and.returnValue(of(''));
    dataServiceSpy.reviewTableUpdate.and.returnValue(of(''));
    dialogSpyObj = jasmine.createSpyObj('MatDialog', ['open']);
    conversionServiceSpy = jasmine.createSpyObj('ConversionService', [], {
      pgSQLToStandardTypeTypeMap: of(new Map<String, String>()),
    })

    await TestBed.configureTestingModule({
    declarations: [ObjectDetailComponent],
    imports: [MatDialogModule,
        MatTableModule,
        MatTabsModule,
        BrowserAnimationsModule,
        MatDividerModule,
        MatIconModule],
    providers: [
        MatSnackBar,
        {
            provide: DataService,
            useValue: dataServiceSpy
        },
        {
            provide: MatDialog,
            useValue: dialogSpyObj
        },
        {
          provide: ConversionService,
          useValue: conversionServiceSpy,
        },
        provideHttpClient(withInterceptorsFromDi())
    ]
}).compileComponents()
    dataServiceSpy.conv = of(mockIConv);
    dataServiceSpy.tableInterleaveStatus = of({});
    dataServiceSpy.updateCheckConstraint.and.returnValue(of(''))
    dataServiceSpy.dropTable.and.returnValue(of(''))
    dataServiceSpy.updatePk = jasmine.createSpy('updatePk').and.returnValue(of(''));
    dataServiceSpy.getConversionRate.and.returnValue()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(ObjectDetailComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
    component.tableData = [
      {
        spOrder: 1,
        srcOrder: 1,
        srcColName: 'srccol1',
        srcDataType: 'int',
        spColName: 'spcol1',
        spDataType: 'Number',
        spIsPk: true,
        srcIsPk: true,
        spIsNotNull: true,
        srcIsNotNull: true,
        srcId: '1',
        spId: '1',
        srcColMaxLength: 50,
        spColMaxLength: 50,
        spCassandraOption: '',
        spAutoGen: {
          Name: '',
          GenerationType: ''
        },
        srcAutoGen: {
          Name: '',
          GenerationType: ''
        },
        srcDefaultValue: '',
        spDefaultValue: {
          Value: {
            ExpressionId: '',
            Statement: ''
          },
          IsPresent: false
        },
      },
      {
        spOrder: 2,
        srcOrder: 2,
        srcColName: 'srccol2',
        srcDataType: 'int',
        spColName: 'spcol2',
        spDataType: 'Number',
        spIsPk: false,
        srcIsPk: false,
        spIsNotNull: false,
        srcIsNotNull: false,
        srcId: '2',
        spId: '2',
        srcColMaxLength: 50,
        spColMaxLength: 50,
        spCassandraOption: '',
        spAutoGen: {
          Name: '',
          GenerationType: ''
        },
        srcAutoGen: {
          Name: '',
          GenerationType: ''
        },
        srcDefaultValue: '',
        spDefaultValue: {
          Value: {
            ExpressionId: '',
            Statement: ''
          },
          IsPresent: false
        },
      },
    ]
    component.fkData = [
      {
        srcName: 'fk_1',
        spName: 'fk_1',
        srcColumns: ['col1'],
        spColumns: ['col1'],
        srcReferTable: 'table2',
        spReferTable: 'table2',
        srcReferColumns: ['col2'],
        spReferColumns: ['col2'],
        srcOnDelete: 'CASCADE',
        spOnDelete: 'CASCADE',
        srcOnUpdate: 'NO ACTION',
        spOnUpdate: 'NO ACTION',
        srcFkId: '1',
        spFkId: '1',
        spColIds: ['col1_id'],
        spReferColumnIds: ['col2_id'],
        spReferTableId: 'table2_id',
      },
    ]
    component.ddlStmts = 'some ddl statment'
    component.typeMap = { int: [{ T: 'Number' }, { T: 'string' }] }
    component.isObjectSelected = true
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })

  it('should render default UI when no table is selected', () => {
    component.isObjectSelected = false
    fixture.detectChanges()
    let title = fixture.debugElement.query(By.css('.title'))
    expect(title.nativeElement.textContent).toEqual('OBJECT VIEWER')
  })

  it('should save sequence', () => {
    let formBuilder = new FormBuilder();
    component.spRowArray = formBuilder.array([
      {
        Id: "s2",
        spSeqName: 'Test Sequence',
        spSequenceKind: 'Kind',
        spSkipRangeMax: '10',
        spSkipRangeMin: '1',
        spStartWithCounter: '100'
      }
    ]);
    component.currentObject = { id: 's2' } as FlatNode;
    component.saveSequence();
    expect(dataServiceSpy.updateSequence).toHaveBeenCalled();
  });

  it('should drop sequence and update sidebar', () => {
    const dialogRefSpyObj = jasmine.createSpyObj({
      afterClosed: of(ObjectDetailNodeType.Sequence),
      close: null
    });
    dialogSpyObj.open.and.returnValue(dialogRefSpyObj);

    component.currentObject = { id: 's2', name: 'Sequence Name' } as FlatNode;
    component.isObjectSelected = true;
    spyOn(component.updateSidebar, 'emit');

    component.dropSequence();

    expect(dialogSpyObj.open).toHaveBeenCalledWith(DropObjectDetailDialogComponent, {
      width: '100%',
      minWidth: '50%',
      maxWidth: '75%',
      data: { name: 'Sequence Name', type: ObjectDetailNodeType.Sequence },
    });

    expect(dataServiceSpy.dropSequence).toHaveBeenCalled();
    expect(component.isObjectSelected).toBe(false);
    expect(component.updateSidebar.emit).toHaveBeenCalledWith(true);
    expect(component.currentObject).toBeNull();
  });

  it('should remove an item with a matching deleteIndex from Check Constraint Array', () => {
    spyOn(component, 'setCCRows').and.callThrough()

    component.ccData = [
      {
        srcSno: '',
        srcConstraintName: 'test',
        srcCondition: 't',
        spSno: '1',
        spConstraintName: 'test',
        spConstraintCondition: 'test',
        spExprId:'expr1',
        deleteIndex: 'cc1',
      },
    ]

    const initialLength = component.ccData.length

    component.dropCc({ value: { deleteIndex: 'cc1' } })
    expect(component.ccData.length).toBe(initialLength - 1)
    expect(component.setCCRows).toHaveBeenCalled()
    expect(component.ccDataSource.length).toBe(0)
    expect(component.ccData.length).toBe(0)
  })

  it('should not remove any items if deleteIndex does not exist from Check Constraint Array', () => {
    component.ccData = [
      {
        srcSno: '',
        srcConstraintName: 'test',
        srcCondition: 't',
        spSno: '1',
        spConstraintName: 'test',
        spConstraintCondition: 'test',
        spExprId:'expr1',
        deleteIndex: 'cc1',
      },
    ]

    const initialLength = component.ccData.length

    component.dropCc({ value: { deleteIndex: 'cc2' } })

    expect(component.ccData.length).toBe(initialLength)
  })

  it('should call setCCRows', () => {
    component.ccData = [
      {
        srcSno: '',
        srcConstraintName: 'contraintName',
        srcCondition: 't',
        spSno: '1',
        spConstraintName: 'test',
        spConstraintCondition: 'test',
        spExprId:'expr1',
        deleteIndex: 'cc1',
      },
      {
        srcSno: '',
        srcConstraintName: '',
        srcCondition: 't',
        spSno: '1',
        spConstraintName: 'contraintName',
        spConstraintCondition: 'test',
        spExprId:'expr1',
        deleteIndex: 'cc1',
      },
    ]
    spyOn(component, 'setCCRows').and.callThrough()

    component.dropCc({ value: { deleteIndex: 'cc1' } })

    expect(component.setCCRows).toHaveBeenCalled()
  })

  it('should call addCcColumn', () => {
    component.ccData = [
      {
        srcSno: '',
        srcConstraintName: 'contraintName',
        srcCondition: 't',
        spSno: '1',
        spConstraintName: 'test',
        spConstraintCondition: 'test',
        spExprId:'expr1',
        deleteIndex: 'cc1',
      }
    ]
    spyOn(component, 'addCcColumn').and.callThrough()

    component.addCcColumn()

    expect(component.addCcColumn).toHaveBeenCalled()
  })

  it('should open dialog if there are duplicate constraints', () => {
    spyOn(component, 'setCCRows').and.callThrough()
    component.ccData = [
      {
        srcSno: '',
        srcConstraintName: '',
        srcCondition: '',
        spSno: '1',
        spConstraintName: 'check_1',
        spConstraintCondition: 'age > 18',
        spExprId:'expr1',
        deleteIndex: 'cc1',
      },
      {
        srcSno: '',
        srcConstraintName: '',
        srcCondition: '',
        spSno: '2',
        spConstraintName: 'check_1',
        spConstraintCondition: 'age >= 18',
        spExprId:'expr1',
        deleteIndex: 'cc2',
      },
    ]
    component.setCCRows()
    component.currentObject = { id: 't2', name: 'Sequence Name' } as FlatNode
    component.saveCc();

    expect(dialogSpyObj.open).toHaveBeenCalledWith(InfodialogComponent, jasmine.objectContaining({
      data: {
        message: jasmine.stringMatching(/constraint name or condition is duplicate/),
        type: 'error'
      }
    }));
  });

  it('should call updateCheckConstraint and handle success response', () => {
    spyOn(component, 'setCCRows').and.callThrough()
    component.ccData = [
      {
        srcSno: '',
        srcConstraintName: '',
        srcCondition: '',
        spSno: '1',
        spConstraintName: 'check_1',
        spConstraintCondition: 'age > 18',
        spExprId:'expr1',
        deleteIndex: 'cc1',
      }
    ]
    component.setCCRows()
    component.currentObject = { id: 't2' } as FlatNode

    component.saveCc();

    expect(dataServiceSpy.updateCheckConstraint);
    expect(component.isCcEditMode).toBe(false);

  })

  it('should Edit the existing record and updateCheckConstraint and handle success response', () => {
    spyOn(component, 'setCCRows').and.callThrough()
    component.ccData = [
      {
        srcSno: '',
        srcConstraintName: '',
        srcCondition: '',
        spSno: '1',
        spConstraintName: 'check_1',
        spConstraintCondition: 'age > 18',
        spExprId:'expr1',
        deleteIndex: 'cc1',
      }
    ]
    component.setCCRows()
    component.ccData[0].srcConstraintName = "check_modify"
    component.setCCRows()
    component.currentObject = { id: 't2' } as FlatNode

    component.saveCc();

    expect(dataServiceSpy.updateCheckConstraint);
    expect(component.isCcEditMode).toBe(false);

  })

  it('should display the check constraints perfectly', () => {
    spyOn(component, 'setCCRows').and.callThrough()
    component.ccData = [
      {
        srcSno: '',
        srcConstraintName: '',
        srcCondition: '',
        spSno: '1',
        spConstraintName: 'check_1',
        spConstraintCondition: 'age > 18',
        spExprId:'expr1',
        deleteIndex: 'cc1',
      }
    ]
    fixture.detectChanges()
    component.setCCRows()
    expect(component.ccData.length).toBe(1)
    expect(component.ccDataSource.length).toBe(1);

  })

  it('should show error dialog if updateCheckConstraint response is an error', () => {

    spyOn(component, 'setCCRows').and.callThrough()
    dataServiceSpy.updateCheckConstraint.and.returnValue(of('Error message'));
    component.ccData = [
      {
        srcSno: '',
        srcConstraintName: '',
        srcCondition: '',
        spSno: '1',
        spConstraintName: 'check_1',
        spConstraintCondition: 'age > 18',
        spExprId:'expr1',
        deleteIndex: 'cc1',
      }
    ]
    component.setCCRows()
    component.currentObject = { id: 't2' } as FlatNode

    component.saveCc();

    expect(dialogSpyObj.open).toHaveBeenCalledWith(InfodialogComponent, jasmine.objectContaining({
      data: { message: 'Error message', type: 'error' }
    }));

  })

  it('should save column table for Cassandra source', () => {
    component.srcDbName = SourceDbNames.Cassandra
    component.conv.SpDialect = Dialect.GoogleStandardSQLDialect
    component.currentObject = { id: 't1', name: 'test_table' } as FlatNode

    const formBuilder = new FormBuilder()
    component.spRowArray = formBuilder.array([
      formBuilder.group({
        srcId: 'c1',
        spId: 'c1_sp',
        spColName: 'spcol1_renamed',
        spDataType: 'ARRAY<INT64>',
        spIsPk: true,
        spIsNotNull: true,
        spColMaxLength: '',
        spCassandraOption: 'list<bigint>', 
        spAutoGen: { Name: '', GenerationType: '' },
        spDefaultValue: '',
      }),
    ])
    component.tableData = [
      {
        srcId: 'c1',
        spId: 'c1_sp',
        spColName: 'spcol1',
      } as IColumnTabData,
    ]
    component.saveColumnTable()

    const expectedPayload: IUpdateTable = {
      UpdateCols: {
        c1: {
          Add: false,
          Rename: 'spcol1_renamed',
          NotNull: 'ADDED',
          Removed: false,
          ToType: 'INT64',
          MaxColLength: '',
          AutoGen: { Name: '', GenerationType: '' },
          DefaultValue: { IsPresent: false, Value: { ExpressionId: '', Statement: '' } },
        },
      },
    }
    expect(dataServiceSpy.reviewTableUpdate).toHaveBeenCalledWith('t1', expectedPayload)
  })

  it('should log an error and return if interleaveType is null', () => {
    spyOn(console, 'error');
    dataServiceSpy.setInterleave.and.returnValue(of(''));
    component.setInterleave('p1', null, 'CASCADE');
    expect(console.error).toHaveBeenCalledWith('Interleave type cannot be empty');
    expect(dataServiceSpy.setInterleave).not.toHaveBeenCalled();
  });

  it('should call setInterleave and show success dialog on success', () => {
    const tableId = 't1';
    const parentId = 'p1';
    const interleaveType = 'IN';
    const onDelete = 'CASCADE';
    component.currentObject = { id: tableId } as FlatNode;
    dataServiceSpy.setInterleave.and.returnValue(of(''));

    component.setInterleave(parentId, interleaveType, onDelete);

    expect(dataServiceSpy.setInterleave).toHaveBeenCalledWith(tableId, interleaveType, parentId, onDelete);
    expect(dialogSpyObj.open).toHaveBeenCalledWith(InfodialogComponent, {
      data: { message: 'Interleave Added Successfully', type: 'info', title: 'Info' },
      maxWidth: '500px',
    });
  });

  it('should call setInterleave and show error dialog on failure', () => {
    const tableId = 't1';
    const parentId = 'p1';
    const interleaveType = 'IN';
    const onDelete = 'CASCADE';
    const errorMessage = 'Interleave failed';
    component.currentObject = { id: tableId } as FlatNode;
    dataServiceSpy.setInterleave.and.returnValue(of(errorMessage));

    component.setInterleave(parentId, interleaveType, onDelete);

    expect(dataServiceSpy.setInterleave).toHaveBeenCalledWith(tableId, interleaveType, parentId, onDelete);
    expect(dialogSpyObj.open).toHaveBeenCalledWith(InfodialogComponent, {
      data: { message: errorMessage, type: 'error', title: 'Error' },
      maxWidth: '500px',
    });
  });

  it('should drop table successfully', () => {
    const dialogRefSpyObj = jasmine.createSpyObj({ afterClosed: of(ObjectDetailNodeType.Table), close: null });
    dialogSpyObj.open.and.returnValue(dialogRefSpyObj);
    component.currentObject = { id: 't1', name: 'test_table' } as FlatNode;
    spyOn(component.updateSidebar, 'emit');
    spyOn(component, 'tableInterleaveWith').and.returnValue([]);

    component.dropTable();

    expect(dialogSpyObj.open).toHaveBeenCalledWith(DropObjectDetailDialogComponent, jasmine.any(Object));
    expect(dataServiceSpy.dropTable).toHaveBeenCalledWith('t1');
    expect(component.isObjectSelected).toBe(false);
    expect(dataServiceSpy.getConversionRate).toHaveBeenCalled();
    expect(component.updateSidebar.emit).toHaveBeenCalledWith(true);
    expect(component.currentObject).toBeNull();
  });

  it('should not drop table if it is interleaved', () => {
    component.currentObject = { id: 't1', name: 'test_table' } as FlatNode;
    spyOn(component, 'tableInterleaveWith').and.returnValue(['t2']);
    component.conv.SpSchema['t2'] = { Name: 'table2' } as any;

    component.dropTable();

    expect(dialogSpyObj.open).toHaveBeenCalledWith(InfodialogComponent, {
      data: {
        message: `Cannot drop the table as it has interleaving with table2. Remove the interleaving first to continue.`,
        title: 'Error',
        type: 'error',
      },
      maxWidth: '500px',
    });
    expect(dataServiceSpy.dropTable).not.toHaveBeenCalled();
  });

  describe('tableInterleaveWith', () => {

    it('should return an empty array if the table is not interleaved with any other table', () => {
      component.conv.SpSchema = {
        't1': { ParentTable: { Id: '' } } as any,
        't2': { ParentTable: { Id: '' } } as any,
      };
      const result = component.tableInterleaveWith('t1');
      expect(result).toEqual([]);
    });

    it('should return child table IDs if the given table is a parent', () => {
      component.conv.SpSchema = {
        't1': { ParentTable: { Id: '' } } as any,
        't2': { ParentTable: { Id: 't1' } } as any,
        't3': { ParentTable: { Id: 't1' } } as any,
        't4': { ParentTable: { Id: 't2' } } as any,
      };
      const result = component.tableInterleaveWith('t1');
      expect(result.sort()).toEqual(['t2', 't3'].sort());
    });

    it('should return the parent table ID if the given table is a child', () => {
      component.conv.SpSchema = {
        't1': { ParentTable: { Id: '' } } as any,
        't2': { ParentTable: { Id: 't1' } } as any,
      };
      const result = component.tableInterleaveWith('t2');
      expect(result).toEqual(['t1']);
    });

    it('should return both parent and child table IDs if the table is in the middle of an interleave chain', () => {
      component.conv.SpSchema = {
        't1': { ParentTable: { Id: '' } } as any,
        't2': { ParentTable: { Id: 't1' } } as any,
        't3': { ParentTable: { Id: 't2' } } as any,
        't4': { ParentTable: { Id: '' } } as any,
      };
      const result = component.tableInterleaveWith('t2');
      expect(result.sort()).toEqual(['t1', 't3'].sort());
    });
  });

  describe('savePk', () => {
    let formBuilder: FormBuilder;

    beforeEach(() => {
      formBuilder = new FormBuilder();
      component.currentObject = { id: 't1', name: 'test_table' } as FlatNode;
      component.conv.SpSchema['t1'] = {
        Id: 't1',
        Name: 'test_table',
        ColDefs: {
          'c1': { Name: 'col1' },
          'c2': { Name: 'col2' }
        },
        PrimaryKeys: [{ ColId: 'c1', Order: 1, Desc: false }]
      } as any;
      component.conv.SpSchema['t2'] = {
        Id: 't2',
        Name: 'table2',
        ColDefs: {
          'c3': { Name: 'col3' },
          'c4': { Name: 'col4' }
        },
        PrimaryKeys: [{ ColId: 'c3', Order: 1 }]
      } as any;
    });

    it('should open error dialog if no PK columns are selected', () => {
      component.pkArray = formBuilder.array([]);
      component.pkData = [];
      component.savePk();

      expect(dialogSpyObj.open).toHaveBeenCalledWith(InfodialogComponent, {
        data: { message: 'Add columns to the primary key for saving', type: 'error' },
        maxWidth: '500px',
      });
      expect(dataServiceSpy.updatePk).not.toHaveBeenCalled();
    });

    it('should call updatePk when there are no interleaved tables', () => {
      component.pkData = [{ spId: 'c1', spIsPk: true, spOrder: 1 }] as IColumnTabData[];
      component.pkArray = formBuilder.array([
        formBuilder.group({ spColName: 'col1', spOrder: 1 })
      ]);
      spyOn(component, 'tableInterleaveWith').and.returnValue([]);

      component.savePk();

      expect(dataServiceSpy.updatePk).toHaveBeenCalledWith({
        TableId: 't1',
        Columns: [{ ColId: 'c1', Desc: false, Order: 1 }]
      });
    });

    it('should call updatePk for interleaved table if PK prefix is not modified', () => {
      component.pkData = [{ spId: 'c1', spIsPk: true, spOrder: 1 }] as IColumnTabData[];
      component.pkArray = formBuilder.array([
        formBuilder.group({ spColName: 'col1', spOrder: 1 })
      ]);
      spyOn(component, 'tableInterleaveWith').and.returnValue(['t2']);
      spyOn(component, 'isPKPrefixModified').and.returnValue(false);

      component.savePk();

      expect(dataServiceSpy.updatePk).toHaveBeenCalled();
      expect(dialogSpyObj.open).not.toHaveBeenCalledWith(InfodialogComponent, jasmine.any(Object));
    });

    it('should show error dialog if PK prefix is modified for an interleaved table', () => {
      component.pkData = [{ spId: 'c2', spIsPk: true, spOrder: 1 }] as IColumnTabData[];
      component.pkArray = formBuilder.array([
        formBuilder.group({ spColName: 'col2', spOrder: 1 })
      ]);
      spyOn(component, 'tableInterleaveWith').and.returnValue(['t2']);
      spyOn(component, 'isPKPrefixModified').and.returnValue(true);

      component.savePk();

      expect(dialogSpyObj.open).toHaveBeenCalledWith(InfodialogComponent, {
        data: {
          message: 'Cannot update primary key as this primary key is part of interleaving with table(s) table2. Please remove the interleaved relationship and try again.',
          type: 'error',
        },
        maxWidth: '500px',
      });
      expect(dataServiceSpy.updatePk).not.toHaveBeenCalled();
    });

    it('should show error dialog if updatePk fails', () => {
      const errorMessage = 'PK update failed';
      dataServiceSpy.updatePk.and.returnValue(of(errorMessage));
      component.pkData = [{ spId: 'c1', spIsPk: true, spOrder: 1 }] as IColumnTabData[];
      component.pkArray = formBuilder.array([
        formBuilder.group({ spColName: 'col1', spOrder: 1 })
      ]);
      spyOn(component, 'tableInterleaveWith').and.returnValue([]);

      component.savePk();

      expect(dialogSpyObj.open).toHaveBeenCalledWith(InfodialogComponent, {
        data: { message: errorMessage, type: 'error' },
        maxWidth: '500px',
      });
      expect(component.isPkEditMode).toBe(true);
    });
  });
});
