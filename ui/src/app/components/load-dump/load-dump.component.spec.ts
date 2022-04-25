import { ComponentFixture, TestBed } from '@angular/core/testing'
import { HttpClientModule } from '@angular/common/http'
import { LoadDumpComponent } from './load-dump.component'
import { RouterModule, Routes } from '@angular/router'
import { WorkspaceComponent } from '../workspace/workspace.component'
import { ReactiveFormsModule } from '@angular/forms'
import { MatCardModule } from '@angular/material/card'
import { MatOptionModule } from '@angular/material/core'
import { MatFormFieldModule } from '@angular/material/form-field'
import { MatInputModule } from '@angular/material/input'
import { MatSelectModule } from '@angular/material/select'
import { BrowserAnimationsModule } from '@angular/platform-browser/animations'
import { By } from '@angular/platform-browser'
import { DebugElement } from '@angular/core'
const appRoutes: Routes = [{ path: 'workspace', component: WorkspaceComponent }]

describe('LoadDumpComponent', () => {
  let component: LoadDumpComponent
  let fixture: ComponentFixture<LoadDumpComponent>
  let btn: DebugElement
  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [LoadDumpComponent],
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
    fixture = TestBed.createComponent(LoadDumpComponent)
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
