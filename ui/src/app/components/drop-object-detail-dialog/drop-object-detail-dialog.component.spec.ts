import { ComponentFixture, TestBed } from '@angular/core/testing'
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog'

import { DropObjectDetailDialogComponent } from './drop-object-detail-dialog.component'

describe('DropObjectDetailDialogComponent', () => {
  let component: DropObjectDetailDialogComponent
  let fixture: ComponentFixture<DropObjectDetailDialogComponent>

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [DropObjectDetailDialogComponent],
      providers: [
        {
          provide: MatDialogRef,
          useValue: {
            close: () => {},
          },
        },
        {
          provide: MAT_DIALOG_DATA,
          useValue: {
          }
        }
      ],
    }).compileComponents()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(DropObjectDetailDialogComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })
})
