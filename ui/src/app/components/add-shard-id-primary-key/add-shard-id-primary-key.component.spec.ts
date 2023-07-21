import { ComponentFixture, TestBed } from '@angular/core/testing';

import { AddShardIdPrimaryKeyComponent } from './add-shard-id-primary-key.component';

describe('AddShardIdPrimaryKeyComponent', () => {
  let component: AddShardIdPrimaryKeyComponent;
  let fixture: ComponentFixture<AddShardIdPrimaryKeyComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ AddShardIdPrimaryKeyComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(AddShardIdPrimaryKeyComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
