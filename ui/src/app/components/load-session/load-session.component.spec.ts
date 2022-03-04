import { ComponentFixture, TestBed } from '@angular/core/testing';

import { LoadSessionComponent } from './load-session.component';

describe('LoadSessionComponent', () => {
  let component: LoadSessionComponent;
  let fixture: ComponentFixture<LoadSessionComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ LoadSessionComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(LoadSessionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
