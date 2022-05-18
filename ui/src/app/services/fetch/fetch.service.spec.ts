import { HttpClientModule } from '@angular/common/http'
import { TestBed } from '@angular/core/testing'

import { FetchService } from './fetch.service'

describe('FetchService', () => {
  let service: FetchService

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientModule],
    })
    service = TestBed.inject(FetchService)
  })

  it('should be created', () => {
    expect(service).toBeTruthy()
  })
})
