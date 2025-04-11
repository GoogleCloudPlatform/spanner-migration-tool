import { provideHttpClient, withInterceptorsFromDi } from '@angular/common/http';
import { ComponentFixture, TestBed } from '@angular/core/testing';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';

import { TuneDatastreamFormComponent } from './tune-datastream-form.component';

describe('TuneDatastreamFormComponent', () => {
  let component: TuneDatastreamFormComponent;
  let fixture: ComponentFixture<TuneDatastreamFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
    declarations: [TuneDatastreamFormComponent],
    imports: [],
    providers: [
        {
            provide: MatDialogRef,
            useValue: {
                close: () => { },
            },
        },
        {
            provide: MAT_DIALOG_DATA,
            useValue: {}
        },
        provideHttpClient(withInterceptorsFromDi())
    ]
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
