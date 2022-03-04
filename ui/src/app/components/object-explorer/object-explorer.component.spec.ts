import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ObjectExplorerComponent } from './object-explorer.component';

describe('ObjectExplorerComponent', () => {
  let component: ObjectExplorerComponent;
  let fixture: ComponentFixture<ObjectExplorerComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ObjectExplorerComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ObjectExplorerComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
