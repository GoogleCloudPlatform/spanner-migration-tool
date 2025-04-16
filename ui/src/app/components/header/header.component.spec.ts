import { ComponentFixture, TestBed } from '@angular/core/testing'
import { provideHttpClient, withInterceptorsFromDi } from '@angular/common/http'
import { MatDialogModule } from '@angular/material/dialog'
import { HeaderComponent } from './header.component'
import { MatSnackBarModule } from '@angular/material/snack-bar'

describe('HeaderComponent', () => {
  let component: HeaderComponent
  let fixture: ComponentFixture<HeaderComponent>

  beforeEach(async () => {
    await TestBed.configureTestingModule({
    declarations: [HeaderComponent],
    imports: [MatDialogModule, MatSnackBarModule],
    providers: [provideHttpClient(withInterceptorsFromDi())]
}).compileComponents()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(HeaderComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })
})
