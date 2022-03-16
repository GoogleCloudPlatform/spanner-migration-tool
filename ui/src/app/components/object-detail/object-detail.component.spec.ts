import { HttpClientModule } from '@angular/common/http'
import { ComponentFixture, TestBed } from '@angular/core/testing'
import { MatDialogModule } from '@angular/material/dialog'
import { ObjectDetailComponent } from './object-detail.component'

describe('ObjectDetailComponent', () => {
  let component: ObjectDetailComponent
  let fixture: ComponentFixture<ObjectDetailComponent>

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ObjectDetailComponent],
      imports: [HttpClientModule, MatDialogModule],
    }).compileComponents()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(ObjectDetailComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })
})
