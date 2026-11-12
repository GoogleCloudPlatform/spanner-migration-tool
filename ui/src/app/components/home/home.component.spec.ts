import { NO_ERRORS_SCHEMA } from '@angular/core';
import { provideHttpClient, withInterceptorsFromDi } from '@angular/common/http'
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { ComponentFixture, TestBed } from '@angular/core/testing'
import { MatDialogModule, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog'
import { MatMenuModule } from '@angular/material/menu'
import { RouterModule, Routes } from '@angular/router'
import { WorkspaceComponent } from '../workspace/workspace.component'
import { HomeComponent } from './home.component'
const appRoutes: Routes = [{ path: 'workspace', component: WorkspaceComponent }]

describe('HomeComponent', () => {
  let component: HomeComponent
  let fixture: ComponentFixture<HomeComponent>

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      schemas: [NO_ERRORS_SCHEMA],
    declarations: [HomeComponent],
    imports: [MatMenuModule, MatDialogModule, RouterModule.forRoot(appRoutes)],
    providers: [
        {
            provide: MatDialogRef,
            useValue: {
                close: () => { },
            },
        },
        {
            provide: MAT_DIALOG_DATA,
            useValue: {}
        },
        provideHttpClient(withInterceptorsFromDi()), provideHttpClientTesting()
    ]
}).compileComponents()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(HomeComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })
})
