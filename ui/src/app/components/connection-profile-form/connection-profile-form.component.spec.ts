import { ComponentFixture, TestBed } from '@angular/core/testing';

import { ConnectionProfileFormComponent } from './connection-profile-form.component';

describe('ConnectionProfileFormComponent', () => {
  let component: ConnectionProfileFormComponent;
  let fixture: ComponentFixture<ConnectionProfileFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ ConnectionProfileFormComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(ConnectionProfileFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
