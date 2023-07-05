import { ComponentFixture, TestBed } from '@angular/core/testing';

import { BulkDropRestoreTableDialogComponent } from './bulk-drop-restore-table-dialog.component';

describe('BulkDropRestoreTableDialogComponent', () => {
  let component: BulkDropRestoreTableDialogComponent;
  let fixture: ComponentFixture<BulkDropRestoreTableDialogComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ BulkDropRestoreTableDialogComponent ]
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
});
