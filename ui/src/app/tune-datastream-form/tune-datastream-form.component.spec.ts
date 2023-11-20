import { ComponentFixture, TestBed } from '@angular/core/testing';

import { TuneDatastreamFormComponent } from './tune-datastream-form.component';

describe('TuneDatastreamFormComponent', () => {
  let component: TuneDatastreamFormComponent;
  let fixture: ComponentFixture<TuneDatastreamFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ TuneDatastreamFormComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(TuneDatastreamFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
