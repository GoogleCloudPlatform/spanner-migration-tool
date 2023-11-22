import { HttpClientModule } from '@angular/common/http';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MatSnackBarModule } from '@angular/material/snack-bar';

import { ShardedBulkSourceDetailsFormComponent } from './sharded-bulk-source-details-form.component';

describe('ShardedBulkSourceDetailsFormComponent', () => {
  let component: ShardedBulkSourceDetailsFormComponent;
  let fixture: ComponentFixture<ShardedBulkSourceDetailsFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ShardedBulkSourceDetailsFormComponent ],
      imports: [ HttpClientModule, MatSnackBarModule ],
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
    fixture = TestBed.createComponent(ShardedBulkSourceDetailsFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
