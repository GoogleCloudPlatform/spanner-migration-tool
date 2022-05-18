import { TestBed } from '@angular/core/testing'

import { SidenavService } from './sidenav.service'

describe('SidenavService', () => {
  let service: SidenavService

  beforeEach(() => {
    TestBed.configureTestingModule({})
    service = TestBed.inject(SidenavService)
  })

  it('should be created', () => {
    expect(service).toBeTruthy()
  })
})
