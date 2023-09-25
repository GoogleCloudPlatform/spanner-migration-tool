import { ComponentFixture, TestBed } from '@angular/core/testing';

import { EquivalentGcloudCommandComponent } from './equivalent-gcloud-command.component';

describe('EquivalentGcloudCommandComponent', () => {
  let component: EquivalentGcloudCommandComponent;
  let fixture: ComponentFixture<EquivalentGcloudCommandComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ EquivalentGcloudCommandComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(EquivalentGcloudCommandComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
