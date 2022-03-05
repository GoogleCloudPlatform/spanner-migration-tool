import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SessionListingComponent } from './session-listing.component';

describe('SessionListingComponent', () => {
  let component: SessionListingComponent;
  let fixture: ComponentFixture<SessionListingComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ SessionListingComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SessionListingComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
