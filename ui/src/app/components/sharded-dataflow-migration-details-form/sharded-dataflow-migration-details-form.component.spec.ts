import { HttpClientModule } from '@angular/common/http';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { MatCardModule } from '@angular/material/card';
import { MatDialogModule, MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MatIconModule } from '@angular/material/icon';
import { MatInputModule } from '@angular/material/input';
import { MatRadioModule } from '@angular/material/radio';
import { MatSelectModule } from '@angular/material/select';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { ShardedDataflowMigrationDetailsFormComponent } from './sharded-dataflow-migration-details-form.component';

describe('ShardedDataflowMigrationDetailsFormComponent', () => {
  let component: ShardedDataflowMigrationDetailsFormComponent;
  let fixture: ComponentFixture<ShardedDataflowMigrationDetailsFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ShardedDataflowMigrationDetailsFormComponent ],
      imports: [ HttpClientModule, MatSnackBarModule, ReactiveFormsModule, MatDialogModule, MatInputModule, MatSelectModule, MatIconModule, MatCardModule, MatRadioModule, BrowserAnimationsModule ],
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
    fixture = TestBed.createComponent(ShardedDataflowMigrationDetailsFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
