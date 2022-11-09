import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TableColumnChangesPreviewComponent } from './table-column-changes-preview.component';

describe('TableColumnChangesPreviewComponent', () => {
  let component: TableColumnChangesPreviewComponent;
  let fixture: ComponentFixture<TableColumnChangesPreviewComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ TableColumnChangesPreviewComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TableColumnChangesPreviewComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
