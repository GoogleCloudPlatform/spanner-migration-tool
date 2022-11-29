import { ComponentFixture, TestBed } from '@angular/core/testing';

import { EndMigrationComponent } from './end-migration.component';

describe('EndMigrationComponent', () => {
  let component: EndMigrationComponent;
  let fixture: ComponentFixture<EndMigrationComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ EndMigrationComponent ]
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
