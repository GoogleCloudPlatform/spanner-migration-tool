import { NO_ERRORS_SCHEMA } from '@angular/core';
import { ComponentFixture, TestBed } from '@angular/core/testing'

import { UpdateSpannerConfigFormComponent } from './update-spanner-config-form.component'
import { provideHttpClient, withInterceptorsFromDi } from '@angular/common/http'
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { MatSnackBarModule } from '@angular/material/snack-bar'
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog'
import { throwError } from 'rxjs'
import { DataService } from 'src/app/services/data/data.service'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { SnackbarService } from 'src/app/services/snackbar/snackbar.service'

describe('UpdateSpannerConfigFormComponent', () => {
  let component: UpdateSpannerConfigFormComponent
  let fixture: ComponentFixture<UpdateSpannerConfigFormComponent>
  let dialogRefSpy: { close: ReturnType<typeof vi.fn> };
  let snackSpy: { openSnackBar: ReturnType<typeof vi.fn> };
  let dataServiceSpy: {
    updateIsOffline: ReturnType<typeof vi.fn>;
    updateConfig: ReturnType<typeof vi.fn>;
    getAllSessions: ReturnType<typeof vi.fn>;
  };
  let fetchServiceSpy: { setSpannerConfig: ReturnType<typeof vi.fn> };

  beforeEach(async () => {
    dialogRefSpy = { close: vi.fn() };
    snackSpy = { openSnackBar: vi.fn() };
    dataServiceSpy = {
      updateIsOffline: vi.fn(),
      updateConfig: vi.fn(),
      getAllSessions: vi.fn(),
    };
    fetchServiceSpy = { setSpannerConfig: vi.fn() };
    await TestBed.configureTestingModule({
      schemas: [NO_ERRORS_SCHEMA],
      declarations: [UpdateSpannerConfigFormComponent],
      imports: [MatSnackBarModule],
      providers: [
        { provide: MAT_DIALOG_DATA, useValue: {} },
        { provide: MatDialogRef, useValue: dialogRefSpy },
        { provide: SnackbarService, useValue: snackSpy },
        { provide: DataService, useValue: dataServiceSpy },
        { provide: FetchService, useValue: fetchServiceSpy },
        provideHttpClient(withInterceptorsFromDi()), provideHttpClientTesting(),
      ]
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
    component.updateConfigForm.setValue({
      GCPProjectID: 'mockProjectID',
      SpannerInstanceID: 'mockInstanceID',
    });

    fetchServiceSpy.setSpannerConfig.mockReturnValue(throwError(() => new Error('Mock error message')))

    component.updateSpannerConfig();

    expect(fetchServiceSpy.setSpannerConfig).toHaveBeenCalledTimes(1);
    expect(fetchServiceSpy.setSpannerConfig).toHaveBeenCalledWith({
      GCPProjectID: 'mockProjectID',
      SpannerInstanceID: 'mockInstanceID',
    });
    expect(snackSpy.openSnackBar).toHaveBeenCalledWith('Mock error message', 'Close');
  });
})
