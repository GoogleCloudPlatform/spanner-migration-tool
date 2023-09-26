import { ComponentFixture, TestBed } from '@angular/core/testing'
import { MatTreeModule } from '@angular/material/tree'
import { MatIconModule } from '@angular/material/icon'
import { ObjectExplorerComponent } from './object-explorer.component'
import { ConversionService } from '../../services/conversion/conversion.service'
import { MatTableModule } from '@angular/material/table'
import { FormsModule } from '@angular/forms'
import { HttpClientModule } from '@angular/common/http'
import { MatDialogModule } from '@angular/material/dialog'
import { MatSnackBarModule } from '@angular/material/snack-bar'

describe('ObjectExplorerComponent', () => {
  let component: ObjectExplorerComponent
  let fixture: ComponentFixture<ObjectExplorerComponent>

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ObjectExplorerComponent],
      providers: [ConversionService],
      imports: [MatTreeModule, MatIconModule, MatTableModule, FormsModule, HttpClientModule, MatDialogModule, MatSnackBarModule],
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
        currentValue: { tab1: 'EXCELLENT', pqr: 'POOR' },
        previousValue: [],
        firstChange: false,
      },
    })
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })
})
