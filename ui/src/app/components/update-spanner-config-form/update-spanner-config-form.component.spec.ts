import { ComponentFixture, TestBed } from '@angular/core/testing';

import { UpdateSpannerConfigFormComponent } from './update-spanner-config-form.component';

describe('UpdateSpannerConfigFormComponent', () => {
  let component: UpdateSpannerConfigFormComponent;
  let fixture: ComponentFixture<UpdateSpannerConfigFormComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ UpdateSpannerConfigFormComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(UpdateSpannerConfigFormComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
