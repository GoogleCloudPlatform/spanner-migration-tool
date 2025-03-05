import { HttpClientModule } from '@angular/common/http'
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
import mockIConv from 'src/mocks/conv'
import { of } from 'rxjs'
import { FormBuilder } from '@angular/forms'
import { FlatNode } from 'src/app/model/schema-object-node'
import { DropObjectDetailDialogComponent } from '../drop-object-detail-dialog/drop-object-detail-dialog.component'
import { ObjectDetailNodeType } from 'src/app/app.constants'
import { InfodialogComponent } from '../infodialog/infodialog.component'

describe('ObjectDetailComponent', () => {
  let component: ObjectDetailComponent
  let fixture: ComponentFixture<ObjectDetailComponent>
  let dataServiceSpy: jasmine.SpyObj<DataService>;
  let dialogSpyObj: jasmine.SpyObj<MatDialog>;
  let rowData: IColumnTabData[]

  beforeEach(async () => {
    dataServiceSpy = jasmine.createSpyObj('DataService', ['updateSequence', 'dropSequence', 'updateCheckConstraint']);
    dataServiceSpy.updateSequence.and.returnValue(of({}));
    dataServiceSpy.dropSequence.and.returnValue(of(''));
    dialogSpyObj = jasmine.createSpyObj('MatDialog', ['open']);

    await TestBed.configureTestingModule({
      declarations: [ObjectDetailComponent],
      providers: [
        MatSnackBar,
        {
          provide: DataService,
          useValue: dataServiceSpy
        },
        {
          provide: MatDialog,
          useValue: dialogSpyObj
        }
      ],
      imports: [
        HttpClientModule,
        MatDialogModule,
        MatTableModule,
        MatTabsModule,
        BrowserAnimationsModule,
        MatDividerModule,
        MatIconModule,
      ],
    }).compileComponents()
    dataServiceSpy.conv = of(mockIConv);
    dataServiceSpy.updateCheckConstraint.and.returnValue(of(''))
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


});
