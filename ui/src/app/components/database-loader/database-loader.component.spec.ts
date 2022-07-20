import { ComponentFixture, TestBed } from '@angular/core/testing';

import { DatabaseLoaderComponent } from './database-loader.component';

describe('DatabaseLoaderComponent', () => {
  let component: DatabaseLoaderComponent;
  let fixture: ComponentFixture<DatabaseLoaderComponent>;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ DatabaseLoaderComponent ]
    })
    .compileComponents();
  });

  beforeEach(() => {
    fixture = TestBed.createComponent(DatabaseLoaderComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
