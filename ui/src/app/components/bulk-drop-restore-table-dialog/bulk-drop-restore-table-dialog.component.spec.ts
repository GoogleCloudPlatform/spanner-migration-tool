import { ComponentFixture, TestBed } from '@angular/core/testing';
import { FormControl, ReactiveFormsModule } from '@angular/forms';
import { MatDialogModule, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';

import { BulkDropRestoreTableDialogComponent } from './bulk-drop-restore-table-dialog.component';

describe('BulkDropRestoreTableDialogComponent', () => {
  let component: BulkDropRestoreTableDialogComponent;
  let fixture: ComponentFixture<BulkDropRestoreTableDialogComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ BulkDropRestoreTableDialogComponent ],
      imports: [MatDialogModule, ReactiveFormsModule],
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
            tables: [
              { TableName: 'Table1', isDeleted: false },
              { TableName: 'Table2', isDeleted: true },
            ],
            operation: 'SKIP'
          }
        }
      ],
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(BulkDropRestoreTableDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should create a confirmation input control with the correct pattern', () => {
    // Test the confirmationInput control creation
    expect(component.confirmationInput instanceof FormControl).toBe(true);
    expect(component.confirmationInput.valid).toBe(false); // Confirm it's invalid initially
    expect(component.confirmationInput.hasError('required')).toBe(true); // Confirm it requires a value
    // Test the pattern validation
    component.confirmationInput.setValue('SKIP');
    expect(component.confirmationInput.valid).toBe(true); // Confirm it's valid with 'SKIP'
    component.confirmationInput.setValue('INVALID_VALUE');
    expect(component.confirmationInput.valid).toBe(false); // Confirm it's invalid with an invalid value
  });

  it('should initialize eligible and ineligible tables based on the provided data', () => {
    // Test the initialization of eligible and ineligible tables
    expect(component.eligibleTables).toEqual(['Table1']);
    expect(component.ineligibleTables).toEqual(['Table2']);
  });
});
