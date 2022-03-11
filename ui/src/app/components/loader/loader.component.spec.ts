import { ComponentFixture, TestBed } from '@angular/core/testing'
import { LoaderService } from 'src/app/services/loader/loader.service'

import { LoaderComponent } from './loader.component'

describe('LoaderComponent', () => {
  let component: LoaderComponent
  let fixture: ComponentFixture<LoaderComponent>
  var loader = new LoaderService()

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      declarations: [LoaderComponent],
    }).compileComponents()
  })

  beforeEach(() => {
    fixture = TestBed.createComponent(LoaderComponent)
    component = fixture.componentInstance
    fixture.detectChanges()
  })

  it('should create', () => {
    expect(component).toBeTruthy()
  })

  it('Check show progress', () => {
    loader.startLoader
    expect(component.showProgress).toEqual(true)
    loader.stopLoader
    expect(component.showProgress).toEqual(false)
  })
})
