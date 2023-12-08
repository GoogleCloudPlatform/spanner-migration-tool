import { ComponentFixture, TestBed } from '@angular/core/testing';
import { RouterModule, Routes } from '@angular/router';
import { WorkspaceComponent } from '../workspace/workspace.component';
import { DatabaseLoaderComponent } from './database-loader.component';
const appRoutes: Routes = [{ path: 'workspace', component: WorkspaceComponent }]

describe('DatabaseLoaderComponent', () => {
  let component: DatabaseLoaderComponent;
  let fixture: ComponentFixture<DatabaseLoaderComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ DatabaseLoaderComponent ],
      imports: [RouterModule.forRoot(appRoutes),]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DatabaseLoaderComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
