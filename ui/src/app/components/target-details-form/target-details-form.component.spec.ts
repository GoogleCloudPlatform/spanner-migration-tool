import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { NgSelectModule } from '@ng-select/ng-select';
import { MigrationDetails, TargetDetails } from 'src/app/app.constants';

import { TargetDetailsFormComponent } from './target-details-form.component';

describe('TargetDetailsFormComponent', () => {
  let component: TargetDetailsFormComponent;
  let fixture: ComponentFixture<TargetDetailsFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ TargetDetailsFormComponent ],
      imports: [ReactiveFormsModule, MatSnackBarModule, NgSelectModule],
      providers: [
        {
          provide: MatDialogRef,
          useValue: {
            close: () => {},
          },
        },
        {
          provide: MAT_DIALOG_DATA,
          useValue: {
          }
        }
      ],
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TargetDetailsFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should persist defaultTransactionIsolation when selected and remove when cleared', () => {
    const setItemSpy = spyOn(localStorage, 'setItem');
    const removeItemSpy = spyOn(localStorage, 'removeItem');

    component.targetDetailsForm.setValue({
      targetDb: 'test-db',
      defaultTimezone: 'America/New_York',
      defaultTransactionIsolation: 'repeatable read',
    });
    component.updateTargetDetails();

    expect(setItemSpy).toHaveBeenCalledWith(TargetDetails.DefaultTransactionIsolation, 'repeatable read');
    expect(setItemSpy).toHaveBeenCalledWith(MigrationDetails.IsTargetDetailSet, 'true');

    component.targetDetailsForm.setValue({
      targetDb: 'test-db',
      defaultTimezone: '',
      defaultTransactionIsolation: '',
    });
    component.updateTargetDetails();

    expect(removeItemSpy).toHaveBeenCalledWith(TargetDetails.DefaultTimezone);
    expect(removeItemSpy).toHaveBeenCalledWith(TargetDetails.DefaultTransactionIsolation);
  });
});
