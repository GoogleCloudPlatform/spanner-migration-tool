import { ComponentFixture, TestBed } from '@angular/core/testing'
import { MatDialogModule, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog'
import { InfodialogComponent } from './infodialog.component'

describe('InfodialogComponent', () => {
  let component: InfodialogComponent
  let fixture: ComponentFixture<InfodialogComponent>

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [InfodialogComponent],
      imports: [MatDialogModule],
      providers: [
        { provide: MAT_DIALOG_DATA, useValue: {} },
        { provide: MatDialogRef, useValue: {} },
      ],
    }).compileComponents()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(InfodialogComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })
})
