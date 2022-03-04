import { ComponentFixture, TestBed } from '@angular/core/testing';

import { LoadDumpComponent } from './load-dump.component';

describe('LoadDumpComponent', () => {
  let component: LoadDumpComponent;
  let fixture: ComponentFixture<LoadDumpComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ LoadDumpComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(LoadDumpComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
