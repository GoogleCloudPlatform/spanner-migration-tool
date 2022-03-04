import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DirectConnectionComponent } from './direct-connection.component';

describe('DirectConnectionComponent', () => {
  let component: DirectConnectionComponent;
  let fixture: ComponentFixture<DirectConnectionComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ DirectConnectionComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DirectConnectionComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
