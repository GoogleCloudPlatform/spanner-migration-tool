import { ComponentFixture, TestBed } from '@angular/core/testing'

import { SummaryComponent } from './summary.component'
import { MatMenuModule } from '@angular/material/menu'
import { By } from '@angular/platform-browser'
import { HttpClientModule } from '@angular/common/http'
import { MatSnackBar } from '@angular/material/snack-bar'
import { ObjectExplorerNodeType } from 'src/app/app.constants'
import { MatAutocompleteModule } from '@angular/material/autocomplete'

fdescribe('SummaryComponent', () => {
  let component: SummaryComponent
  let fixture: ComponentFixture<SummaryComponent>

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [SummaryComponent],
      providers: [MatSnackBar],
      imports: [MatMenuModule, MatAutocompleteModule, HttpClientModule],
    }).compileComponents()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(SummaryComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
    component.options = ['read', 'unread', 'warning', 'suggestion', 'note']
    component.currentObject = {
      expandable: true,
      isSpannerNode: true,
      level: 2,
      name: 'cart',
      parent: '',
      parentId: '',
      pos: -1,
      status: 'GREEN',
      type: ObjectExplorerNodeType.Table,
      id: '',
      isDeleted: true,
    }
    component.summaryRows = [
      {
        type: 'warning',
        content: 'qwertyuiop',
        isRead: false,
      },
      {
        type: 'warning',
        content: 'asdfg',
        isRead: false,
      },
      {
        type: 'note',
        content: 'zxcvb',
        isRead: true,
      },
    ]
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })

  it('should render empty message when there is no table', () => {
    component.currentObject = null
    fixture.detectChanges()
    let message = fixture.debugElement.query(By.css('.no-object-message'))
    expect(message.nativeElement.textContent).toEqual(
      ' Click on a converted object name on the Spanner tree panel to view its conversion issues. '
    )
  })

  it('should display only read message with read filter', () => {
    component.searchFilters = ['read']
    fixture.detectChanges()
    component.applyFilters()
    expect(component.filteredSummaryRows).toEqual([
      {
        type: 'note',
        content: 'zxcvb',
        isRead: true,
      },
    ])
  })

  it('should display only warnings with warning filter', () => {
    component.searchFilters = ['warning']
    fixture.detectChanges()
    component.applyFilters()
    expect(component.filteredSummaryRows).toEqual([
      {
        type: 'warning',
        content: 'qwertyuiop',
        isRead: false,
      },
      {
        type: 'warning',
        content: 'asdfg',
        isRead: false,
      },
    ])
  })
  it('should display all notes and warnings with notes and warining filters', () => {
    component.searchFilters = ['warning', 'note']
    fixture.detectChanges()
    component.applyFilters()
    expect(component.filteredSummaryRows).toEqual([
      {
        type: 'warning',
        content: 'qwertyuiop',
        isRead: false,
      },
      {
        type: 'warning',
        content: 'asdfg',
        isRead: false,
      },
      {
        type: 'note',
        content: 'zxcvb',
        isRead: true,
      },
    ])
  })
})
