import { HttpClientModule } from '@angular/common/http'
import { ComponentFixture, TestBed } from '@angular/core/testing'
import { MatDialogModule } from '@angular/material/dialog'
import { By } from '@angular/platform-browser'
import IColumnTabData from 'src/app/model/edit-table'
import { ObjectDetailComponent } from './object-detail.component'
import { MatTableModule } from '@angular/material/table'
import { MatTabsModule } from '@angular/material/tabs'
import { BrowserAnimationsModule } from '@angular/platform-browser/animations'
import { MatDividerModule } from '@angular/material/divider'
import { MatIconModule } from '@angular/material/icon'
import { MatSnackBar } from '@angular/material/snack-bar'
import { ObjectExplorerNodeType } from 'src/app/app.constants'

describe('ObjectDetailComponent', () => {
  let component: ObjectDetailComponent
  let fixture: ComponentFixture<ObjectDetailComponent>
  let rowData: IColumnTabData[]
  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ObjectDetailComponent],
      providers: [MatSnackBar],
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

  it('should render default Ui when no table is selected', () => {
    component.isObjectSelected = false
    fixture.detectChanges()
    let title = fixture.debugElement.query(By.css('.title'))
    expect(title.nativeElement.textContent).toEqual('OBJECT VIEWER')
  })
})
