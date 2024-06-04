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

describe('ObjectDetailComponent', () => {
  let component: ObjectDetailComponent
  let fixture: ComponentFixture<ObjectDetailComponent>
  let dataServiceSpy: jasmine.SpyObj<DataService>;
  let dialogSpyObj: jasmine.SpyObj<MatDialog>;
  let rowData: IColumnTabData[]

  beforeEach(async () => {
    dataServiceSpy = jasmine.createSpyObj('DataService', ['updateSequence', 'dropSequence']);
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
        }
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
        }
      },
    ]
    component.fkData = [
      {
        spName: 'abc',
        srcName: 'abc',
        spColumns: ['abc_id'],
        srcColumns: ['abc_id'],
        spReferTable: 'def',
        srcReferTable: 'def',
        spReferColumns: ['def_if'],
        srcReferColumns: ['def_if'],
        srcFkId: '1',
        spFkId: '1',
        spColIds: ['1'],
        spReferColumnIds: ['1'],
        spReferTableId: '1',
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
});
