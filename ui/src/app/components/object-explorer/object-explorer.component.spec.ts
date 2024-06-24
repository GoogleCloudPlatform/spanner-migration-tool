import { ComponentFixture, TestBed } from '@angular/core/testing'
import { MatTreeModule } from '@angular/material/tree'
import { MatIconModule } from '@angular/material/icon'
import { ObjectExplorerComponent } from './object-explorer.component'
import { ConversionService } from '../../services/conversion/conversion.service'
import { MatTableModule } from '@angular/material/table'
import { FormsModule } from '@angular/forms'
import { HttpClientModule } from '@angular/common/http'
import { MatDialog, MatDialogModule } from '@angular/material/dialog'
import { MatSnackBarModule } from '@angular/material/snack-bar'
import { FlatNode } from 'src/app/model/schema-object-node'
import { ObjectExplorerNodeType } from 'src/app/app.constants'
import { AddNewSequenceComponent } from '../add-new-sequence/add-new-sequence.component'

describe('ObjectExplorerComponent', () => {
  let component: ObjectExplorerComponent
  let fixture: ComponentFixture<ObjectExplorerComponent>
  let dialog: MatDialog;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ObjectExplorerComponent],
      providers: [ConversionService],
      imports: [MatTreeModule, MatIconModule, MatTableModule, FormsModule, HttpClientModule, MatDialogModule, MatSnackBarModule, MatDialogModule],
    }).compileComponents()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(ObjectExplorerComponent)
    component = fixture.componentInstance
    dialog = TestBed.inject(MatDialog);
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

  it('is sequence node', () => {
    expect(component.isSequenceNode('sequences')).toBeTruthy()
    expect(component.isSequenceNode('index')).toBeFalsy()
  })

  it('is table like node', () => {
    let data: FlatNode = {
      expandable: false,
      name: '',
      status: undefined,
      type: ObjectExplorerNodeType.Tables,
      pos: 0,
      level: 0,
      isSpannerNode: false,
      isDeleted: false,
      id: '',
      parent: '',
      parentId: ''
    }
    expect(component.isTableLikeNode(data)).toBeTruthy()
    data.type = ObjectExplorerNodeType.Index
    expect(component.isTableLikeNode(data)).toBeFalsy()
  }) 

  it('is sequence like node', () => {
    let data: FlatNode = {
      expandable: false,
      name: '',
      status: undefined,
      type: ObjectExplorerNodeType.Sequence,
      pos: 0,
      level: 0,
      isSpannerNode: false,
      isDeleted: false,
      id: '',
      parent: '',
      parentId: ''
    }
    expect(component.isSequenceLikeNode(data)).toBeTruthy()
    data.type = ObjectExplorerNodeType.Index
    expect(component.isSequenceLikeNode(data)).toBeFalsy()
  }) 
})
