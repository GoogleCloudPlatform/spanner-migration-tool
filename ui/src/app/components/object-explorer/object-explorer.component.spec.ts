import { ComponentFixture, TestBed } from '@angular/core/testing'
import { MatTreeModule } from '@angular/material/tree'
import { MatIconModule } from '@angular/material/icon'
import { ObjectExplorerComponent } from './object-explorer.component'
import { ConversionService } from '../../services/conversion/conversion.service'
import { MatTableModule } from '@angular/material/table'
import { FormsModule } from '@angular/forms'

describe('ObjectExplorerComponent', () => {
  let component: ObjectExplorerComponent
  let fixture: ComponentFixture<ObjectExplorerComponent>

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ObjectExplorerComponent],
      providers: [ConversionService],
      imports: [MatTreeModule, MatIconModule, MatTableModule, FormsModule],
    }).compileComponents()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(ObjectExplorerComponent)
    component = fixture.componentInstance
    component.ngOnChanges({
      tableNames: {
        isFirstChange: () => false,
        currentValue: ['tab1', 'pqr'],
        previousValue: [],
        firstChange: false,
      },
      conversionRates: {
        isFirstChange: () => false,
        currentValue: { tab1: 'GREEN', pqr: 'ORANGE' },
        previousValue: [],
        firstChange: false,
      },
    })
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })

  it('should create true tree node data ', () => {
    expect(component.tableNames.length).toEqual(2)
    expect(component.dataSource.data.length).toEqual(1)
    expect(component.dataSource.data).toEqual([
      {
        name: 'Database Name',
        children: [
          {
            name: 'Tables',
            children: [
              {
                name: 'tab1',
                status: 'GREEN',
              },
              {
                name: 'pqr',
                status: 'ORANGE',
              },
            ],
          },
        ],
      },
    ])
  })

  it('should  render mat tree  correctly', () => {
    expect(fixture.nativeElement.querySelector('.example-tree')).not.toBeNull()
  })

  it('should render table filtered by search box value (search)', () => {
    component.filterTable('tab1')
    expect(fixture.nativeElement.querySelectorAll('mat-tree-node').length).toEqual(1)
  })

  it('should emit table name when click on a sidebar table name', () => {
    component.selectTable.subscribe((table) => {
      expect(table).toEqual('tab1')
    })
    fixture.nativeElement.querySelector('#tab1').click()
  })
})
