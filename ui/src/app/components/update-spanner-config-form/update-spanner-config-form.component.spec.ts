import { ComponentFixture, TestBed } from '@angular/core/testing'

import { UpdateSpannerConfigFormComponent } from './update-spanner-config-form.component'
import { HttpClientModule } from '@angular/common/http'
import { MatSnackBarModule } from '@angular/material/snack-bar'
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog'
import { throwError } from 'rxjs'
import { DataService } from 'src/app/services/data/data.service'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service'

describe('UpdateSpannerConfigFormComponent', () => {
  let component: UpdateSpannerConfigFormComponent
  let fixture: ComponentFixture<UpdateSpannerConfigFormComponent>
  let dialogRefSpy: jasmine.SpyObj<MatDialogRef<UpdateSpannerConfigFormComponent>>;
  let snackSpy: jasmine.SpyObj<SnackbarService>;
  let dataServiceSpy: jasmine.SpyObj<DataService>;
  let fetchServiceSpy: jasmine.SpyObj<FetchService>;

  beforeEach(async () => {
    dialogRefSpy = jasmine.createSpyObj('MatDialogRef', ['close']);
    snackSpy = jasmine.createSpyObj('SnackbarService', ['openSnackBar']);
    dataServiceSpy = jasmine.createSpyObj('DataService', ['updateIsOffline', 'updateConfig', 'getAllSessions']);
    fetchServiceSpy = jasmine.createSpyObj('FetchService', ['setSpannerConfig']);
    await TestBed.configureTestingModule({
      declarations: [UpdateSpannerConfigFormComponent],
      imports: [HttpClientModule, MatSnackBarModule],
      providers: [
        { provide: MAT_DIALOG_DATA, useValue: {} },
        { provide: MatDialogRef, useValue: dialogRefSpy },
        { provide: SnackbarService, useValue: snackSpy },
        { provide: DataService, useValue: dataServiceSpy },
        { provide: FetchService, useValue: fetchServiceSpy },
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

  it('should handle error in updateSpannerConfig', () => {
    // Mock the form value
    component.updateConfigForm.setValue({
      GCPProjectID: 'mockProjectID',
      SpannerInstanceID: 'mockInstanceID',
    });

    // Mock the error response from setSpannerConfig
    fetchServiceSpy.setSpannerConfig.and.returnValue(throwError(() =>new Error('Mock error message')))

    // Trigger the updateSpannerConfig method
    component.updateSpannerConfig();

    // Assertions
    expect(fetchServiceSpy.setSpannerConfig).toHaveBeenCalledOnceWith({
      GCPProjectID: 'mockProjectID',
      SpannerInstanceID: 'mockInstanceID',
    });
    expect(snackSpy.openSnackBar).toHaveBeenCalledWith('Mock error message', 'Close');
  });
})
