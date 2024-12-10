import { HttpClientModule } from '@angular/common/http'
import { ComponentFixture, TestBed } from '@angular/core/testing'
import { MatDialogModule, MatDialogRef, MAT_DIALOG_DATA, MatDialog } from '@angular/material/dialog'
import { MatMenuModule } from '@angular/material/menu'
import { Router, RouterModule, Routes } from '@angular/router'
import { WorkspaceComponent } from '../workspace/workspace.component'
import { HomeComponent } from './home.component'
import { MatSnackBarModule } from '@angular/material/snack-bar'
import { InfodialogComponent } from '../infodialog/infodialog.component'
import { DataService } from 'src/app/services/data/data.service'
import { BehaviorSubject } from 'rxjs'
const appRoutes: Routes = [{ path: 'workspace', component: WorkspaceComponent }]

describe('HomeComponent', () => {
  let component: HomeComponent
  let fixture: ComponentFixture<HomeComponent>
  let dataService: DataService;
  let dialog: MatDialog;
  let router: Router;
  const isOfflineSubject = new BehaviorSubject<boolean>(true);

  beforeEach(async () => {
    await TestBed.configureTestingModule({

      declarations: [HomeComponent],
      imports: [MatMenuModule, MatDialogModule, RouterModule.forRoot(appRoutes), HttpClientModule,MatSnackBarModule],
      providers: [
        {
          provide: MatDialogRef,
          useValue: {
            close: () => {},
          },
        },
        { provide: DataService, useValue: { isOffline: isOfflineSubject.asObservable() } },
        {
          provide: MAT_DIALOG_DATA,
          useValue: {
          }
        }
      ],
    }).compileComponents()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(HomeComponent)
    dataService = TestBed.inject(DataService);
    dialog = TestBed.inject(MatDialog);
    router = TestBed.inject(Router);
    component = fixture.componentInstance
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })

  it('should set isOfflineStatus when isOffline emits a value', () => {
    isOfflineSubject.next(true); // Emit true
    fixture.detectChanges();
    expect(component.isOfflineStatus).toBeTrue();
  });

  it('should open dialog if isOfflineStatus is true', () => {

    isOfflineSubject.next(true);
    fixture.detectChanges();

    spyOn(dialog, 'open').and.callThrough();

    component.connectToDatabase();

    expect(dialog.open).toHaveBeenCalledWith(InfodialogComponent, {
      data: { message: 'Please configure spanner project id and instance id to proceed', type: 'error', title: 'Configure Spanner' },
      maxWidth: '500px',
    });
  });

  it('should navigate to direct connection if isOfflineStatus is false', () => {
    isOfflineSubject.next(false);
    fixture.detectChanges();

    spyOn(router, 'navigate');

    component.connectToDatabase();

    expect(router.navigate).toHaveBeenCalledWith(['/source/direct-connection']);
  });

})
