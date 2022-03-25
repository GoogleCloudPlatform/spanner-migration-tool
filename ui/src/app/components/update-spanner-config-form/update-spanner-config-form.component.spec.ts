import { ComponentFixture, TestBed } from '@angular/core/testing'

import { UpdateSpannerConfigFormComponent } from './update-spanner-config-form.component'
import { HttpClientModule } from '@angular/common/http'
import { MatSnackBarModule } from '@angular/material/snack-bar'
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog'

describe('UpdateSpannerConfigFormComponent', () => {
  let component: UpdateSpannerConfigFormComponent
  let fixture: ComponentFixture<UpdateSpannerConfigFormComponent>

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [UpdateSpannerConfigFormComponent],
      imports: [HttpClientModule, MatSnackBarModule],
      providers: [
        { provide: MAT_DIALOG_DATA, useValue: {} },
        { provide: MatDialogRef, useValue: {} },
      ],
    }).compileComponents()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(UpdateSpannerConfigFormComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })
})
