import { HttpClientModule } from '@angular/common/http';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { ReactiveFormsModule } from '@angular/forms';
import { MatSelectModule } from '@angular/material/select';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import { BrowserAnimationsModule } from '@angular/platform-browser/animations';

import { AddShardIdPrimaryKeyComponent } from './add-shard-id-primary-key.component';

describe('AddShardIdPrimaryKeyComponent', () => {
  let component: AddShardIdPrimaryKeyComponent;
  let fixture: ComponentFixture<AddShardIdPrimaryKeyComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ AddShardIdPrimaryKeyComponent ],
      imports: [ReactiveFormsModule, HttpClientModule, MatSnackBarModule, MatSelectModule, BrowserAnimationsModule]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AddShardIdPrimaryKeyComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
