import { HttpClientModule } from '@angular/common/http';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { Dataflow } from 'src/app/app.constants';
import ISpannerConfig from 'src/app/model/spanner-config';

import { DataflowFormComponent } from './dataflow-form.component';

describe('DataflowFormComponent', () => {
  let component: DataflowFormComponent;
  let fixture: ComponentFixture<DataflowFormComponent>;
  let dialogRef: MatDialogRef<DataflowFormComponent>;

  const mockData: ISpannerConfig = {
    GCPProjectID: 'test-project-id',
    SpannerInstanceID: ''
  };

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ DataflowFormComponent ],
      imports: [ReactiveFormsModule,HttpClientModule],
      providers: [
        {
          provide: MAT_DIALOG_DATA,
          useValue: mockData
        },
        {
          provide: MatDialogRef,
          useValue: {
            close: jasmine.createSpy('close'),
          },
        }
      ],
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DataflowFormComponent);
    component = fixture.componentInstance;
    dialogRef = TestBed.inject(MatDialogRef);
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
  it('should initialize form groups with default values and validators', () => {
    const tunableFlagsForm = component.tunableFlagsForm;
    const presetFlagsForm = component.presetFlagsForm;

    // Tunable flags form
    expect(tunableFlagsForm.get('vpcHostProjectId')?.value).toBe(mockData.GCPProjectID);
    expect(tunableFlagsForm.get('network')?.value).toBe('');
    expect(tunableFlagsForm.get('subnetwork')?.value).toBe('');
    expect(tunableFlagsForm.get('maxWorkers')?.value).toBe('50');
    expect(tunableFlagsForm.get('numWorkers')?.value).toBe('1');
    expect(tunableFlagsForm.get('machineType')?.value).toBe('n1-standard-2');
    expect(tunableFlagsForm.get('serviceAccountEmail')?.value).toBe('');
    expect(tunableFlagsForm.get('additionalUserLabels')?.value).toBe('');
    expect(tunableFlagsForm.get('kmsKeyName')?.value).toBe('');
    expect(tunableFlagsForm.get('customJarPath')?.value).toBe('');
    expect(tunableFlagsForm.get('customClassName')?.value).toBe('');
    expect(tunableFlagsForm.get('customParameter')?.value).toBe('');

    // Preset flags form
    expect(presetFlagsForm.get('dataflowProjectId')?.value).toBe(mockData.GCPProjectID);
    expect(presetFlagsForm.get('dataflowLocation')?.value).toBe('');
    expect(presetFlagsForm.get('gcsTemplatePath')?.value).toBe('');

    // Validation checks for tunableFlagsForm
    expect(tunableFlagsForm.get('vpcHostProjectId')?.hasError('required')).toBe(false);
    expect(tunableFlagsForm.get('maxWorkers')?.hasError('required')).toBe(false);
    expect(tunableFlagsForm.get('maxWorkers')?.hasError('pattern')).toBe(false);
    expect(tunableFlagsForm.get('numWorkers')?.hasError('required')).toBe(false);
    expect(tunableFlagsForm.get('numWorkers')?.hasError('pattern')).toBe(false);
    expect(tunableFlagsForm.get('machineType')?.hasError('required')).toBe(false);
    expect(tunableFlagsForm.get('additionalUserLabels')?.hasError('pattern')).toBe(false);
    expect(tunableFlagsForm.get('kmsKeyName')?.hasError('pattern')).toBe(false);
    expect(tunableFlagsForm.get('customJarPath')?.hasError('pattern')).toBe(false);

    // Check that the preset flags form is disabled
    expect(presetFlagsForm.disabled).toBe(true);
  });

  it('should update localStorage and close dialog on updateDataflowDetails', () => {
    component.updateDataflowDetails();
    const formValue = component.tunableFlagsForm.value;
    const presetValue = component.presetFlagsForm.value;

    expect(localStorage.getItem(Dataflow.Network)).toBe(formValue.network);
    expect(localStorage.getItem(Dataflow.Subnetwork)).toBe(formValue.subnetwork);
    expect(localStorage.getItem(Dataflow.VpcHostProjectId)).toBe(formValue.vpcHostProjectId);
    expect(localStorage.getItem(Dataflow.MaxWorkers)).toBe(formValue.maxWorkers);
    expect(localStorage.getItem(Dataflow.NumWorkers)).toBe(formValue.numWorkers);
    expect(localStorage.getItem(Dataflow.ServiceAccountEmail)).toBe(formValue.serviceAccountEmail);
    expect(localStorage.getItem(Dataflow.MachineType)).toBe(formValue.machineType);
    expect(localStorage.getItem(Dataflow.AdditionalUserLabels)).toBe(formValue.additionalUserLabels);
    expect(localStorage.getItem(Dataflow.KmsKeyName)).toBe(formValue.kmsKeyName);
    expect(localStorage.getItem(Dataflow.ProjectId)).toBe(presetValue.dataflowProjectId);
    expect(localStorage.getItem(Dataflow.Location)).toBe(presetValue.dataflowLocation);
    expect(localStorage.getItem(Dataflow.GcsTemplatePath)).toBe(presetValue.gcsTemplatePath);
    expect(localStorage.getItem(Dataflow.IsDataflowConfigSet)).toBe("true");
    expect(localStorage.getItem(Dataflow.CustomClassName)).toBe(formValue.customClassName);
    expect(localStorage.getItem(Dataflow.CustomJarPath)).toBe(formValue.customJarPath);
    expect(localStorage.getItem(Dataflow.CustomParameter)).toBe(formValue.customParameter);

    expect(dialogRef.close).toHaveBeenCalled();
  });

  it('should enable preset flags form and set disablePresetFlags to false on enablePresetFlags', () => {
    component.enablePresetFlags();

    expect(component.disablePresetFlags).toBe(false);
    expect(component.presetFlagsForm.enabled).toBe(true);
  });
});
