import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SourceSelectionComponent } from './source-selection.component';

describe('SourceSelectionComponent', () => {
  let component: SourceSelectionComponent;
  let fixture: ComponentFixture<SourceSelectionComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ SourceSelectionComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SourceSelectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
