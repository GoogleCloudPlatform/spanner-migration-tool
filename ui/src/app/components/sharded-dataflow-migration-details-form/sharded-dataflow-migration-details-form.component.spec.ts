import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ShardedDataflowMigrationDetailsFormComponent } from './sharded-dataflow-migration-details-form.component';

describe('ShardedDataflowMigrationDetailsFormComponent', () => {
  let component: ShardedDataflowMigrationDetailsFormComponent;
  let fixture: ComponentFixture<ShardedDataflowMigrationDetailsFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ShardedDataflowMigrationDetailsFormComponent ]
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
