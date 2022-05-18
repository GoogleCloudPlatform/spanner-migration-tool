import { HttpClientModule } from '@angular/common/http'
import { DebugElement } from '@angular/core'
import { ComponentFixture, TestBed } from '@angular/core/testing'
import { ReactiveFormsModule } from '@angular/forms'
import { MatCardModule } from '@angular/material/card'
import { MatOptionModule } from '@angular/material/core'
import { MatFormFieldModule } from '@angular/material/form-field'
import { MatInputModule } from '@angular/material/input'
import { MatSelectModule } from '@angular/material/select'
import { By } from '@angular/platform-browser'
import { BrowserAnimationsModule } from '@angular/platform-browser/animations'
import { RouterModule, Routes } from '@angular/router'
import { WorkspaceComponent } from '../workspace/workspace.component'
import { LoadSessionComponent } from './load-session.component'

const appRoutes: Routes = [{ path: 'workspace', component: WorkspaceComponent }]

describe('LoadSessionComponent', () => {
  let component: LoadSessionComponent
  let fixture: ComponentFixture<LoadSessionComponent>
  let btn: DebugElement
  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [LoadSessionComponent],
      imports: [
        RouterModule.forRoot(appRoutes),
        HttpClientModule,
        ReactiveFormsModule,
        MatCardModule,
        MatSelectModule,
        MatOptionModule,
        MatFormFieldModule,
        MatInputModule,
        BrowserAnimationsModule,
      ],
    }).compileComponents()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(LoadSessionComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
    btn = fixture.debugElement.query(By.css('button[type=submit]'))
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })

  it('should validate all required input and disable submit button is invalid input', () => {
    component.connectForm.setValue({ dbEngine: '', filePath: '' })
    fixture.detectChanges()
    expect(component.connectForm.invalid).toEqual(true)
    expect(btn.nativeElement.disabled).toEqual(true)
  })

  it('should enable button when form is valid', () => {
    component.connectForm.setValue({ dbEngine: 'mysqldump', filePath: '/sql.sql' })
    fixture.detectChanges()
    expect(component.connectForm.invalid).toEqual(false)
    expect(btn.nativeElement.disabled).toEqual(false)
  })
})
