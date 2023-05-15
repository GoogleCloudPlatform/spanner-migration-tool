import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ShardedBulkSourceDetailsFormComponent } from './sharded-bulk-source-details-form.component';

describe('ShardedBulkSourceDetailsFormComponent', () => {
  let component: ShardedBulkSourceDetailsFormComponent;
  let fixture: ComponentFixture<ShardedBulkSourceDetailsFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ShardedBulkSourceDetailsFormComponent ]
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
