import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SidenavRuleComponent } from './sidenav-rule.component';

describe('SidenavRuleComponent', () => {
  let component: SidenavRuleComponent;
  let fixture: ComponentFixture<SidenavRuleComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ SidenavRuleComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(SidenavRuleComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
