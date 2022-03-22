import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SaveSessionFormComponent } from './save-session-form.component';

describe('SaveSessionFormComponent', () => {
  let component: SaveSessionFormComponent;
  let fixture: ComponentFixture<SaveSessionFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ SaveSessionFormComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SaveSessionFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
