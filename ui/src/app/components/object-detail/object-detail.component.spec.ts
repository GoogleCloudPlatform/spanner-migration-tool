import { ComponentFixture, TestBed } from '@angular/core/testing'

import { ObjectDetailComponent } from './object-detail.component'

describe('ObjectDetailComponent', () => {
  let component: ObjectDetailComponent
  let fixture: ComponentFixture<ObjectDetailComponent>

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [ObjectDetailComponent],
    }).compileComponents()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(ObjectDetailComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })
})
