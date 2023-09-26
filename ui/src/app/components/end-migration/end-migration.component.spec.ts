import { HttpClientModule } from '@angular/common/http';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { MatSnackBarModule } from '@angular/material/snack-bar';

import { EndMigrationComponent } from './end-migration.component';

describe('EndMigrationComponent', () => {
  let component: EndMigrationComponent;
  let fixture: ComponentFixture<EndMigrationComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ EndMigrationComponent ],
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
    fixture = TestBed.createComponent(EndMigrationComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
