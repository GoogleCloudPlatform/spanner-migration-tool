import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DropIndexDialogComponent } from './drop-index-dialog.component';

describe('DropIndexDialogComponent', () => {
  let component: DropIndexDialogComponent;
  let fixture: ComponentFixture<DropIndexDialogComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ DropIndexDialogComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DropIndexDialogComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
