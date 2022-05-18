import { TestBed } from '@angular/core/testing'

import { LoaderService } from './loader.service'

describe('LoaderService', () => {
  let service: LoaderService

  beforeEach(() => {
    TestBed.configureTestingModule({})
    service = TestBed.inject(LoaderService)
  })

  it('should be created', () => {
    expect(service).toBeTruthy()
  })

  it('Start Loader', () => {
    service.startLoader
    service.isLoading.subscribe((data) => expect(data).toEqual(false))
  })

  it('Stop loader', () => {
    service.startLoader
    service.isLoading.subscribe((data) => expect(data).toEqual(false))
  })
})
