import { ComponentFixture, TestBed } from '@angular/core/testing'
import { HttpClientModule } from '@angular/common/http'
import { SaveSessionFormComponent } from './save-session-form.component'
import { MatSnackBarModule } from '@angular/material/snack-bar'

describe('SaveSessionFormComponent', () => {
  let component: SaveSessionFormComponent
  let fixture: ComponentFixture<SaveSessionFormComponent>

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [SaveSessionFormComponent],
      imports: [HttpClientModule, MatSnackBarModule],
    }).compileComponents()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(SaveSessionFormComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })
})
