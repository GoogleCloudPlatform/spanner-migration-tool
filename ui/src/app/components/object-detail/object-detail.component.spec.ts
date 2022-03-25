import { HttpClientModule } from '@angular/common/http'
import { ComponentFixture, TestBed } from '@angular/core/testing'
import { MatDialogModule } from '@angular/material/dialog'
import { By } from '@angular/platform-browser'
import IColumnTabData from 'src/app/model/ColumnTabData'
import { ObjectDetailComponent } from './object-detail.component'
import { MatTableModule } from '@angular/material/table'
import { MatTabsModule } from '@angular/material/tabs'
import { BrowserAnimationsModule } from '@angular/platform-browser/animations'
import { MatDividerModule } from '@angular/material/divider'
import { MatIconModule } from '@angular/material/icon'

fdescribe('ObjectDetailComponent', () => {
  let component: ObjectDetailComponent
  let fixture: ComponentFixture<ObjectDetailComponent>
  let rowData: IColumnTabData[]
  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ObjectDetailComponent],
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
    component.rowData = [
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
      },
    ]
    component.tableName = 'test'
    component.ddlStmts = 'some ddl statment'
    component.typeMap = { int: [{ T: 'Number' }, { T: 'string' }] }
    component.isTableSelected = true
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })

  it('should render default Ui when no table is selected', () => {
    component.isTableSelected = false
    fixture.detectChanges()
    let title = fixture.debugElement.query(By.css('.title'))
    expect(title.nativeElement.textContent).toEqual('OBJECT VIEWER')
  })

  it('should table title render correctly', () => {
    fixture.detectChanges()
    let title = fixture.debugElement.query(By.css('.title'))
    expect(title.nativeElement.textContent).toEqual('table_chart test ')
  })

  it('should render table with given data', () => {
    fixture.detectChanges()
    let table = fixture.debugElement.query(By.css('.mat-column-srcOrder'))
    expect(table.nativeElement.textContent).toEqual('Order')
  })
})
