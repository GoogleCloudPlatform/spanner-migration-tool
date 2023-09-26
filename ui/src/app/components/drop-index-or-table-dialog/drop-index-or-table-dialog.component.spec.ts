import { ComponentFixture, TestBed } from '@angular/core/testing'
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog'

import { DropIndexOrTableDialogComponent } from './drop-index-or-table-dialog.component'

describe('DropIndexOrTableDialogComponent', () => {
  let component: DropIndexOrTableDialogComponent
  let fixture: ComponentFixture<DropIndexOrTableDialogComponent>

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [DropIndexOrTableDialogComponent],
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
    fixture = TestBed.createComponent(DropIndexOrTableDialogComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })
})
