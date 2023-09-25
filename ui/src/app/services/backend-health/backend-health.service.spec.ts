import { TestBed } from '@angular/core/testing';

import { BackendHealthService } from './backend-health.service';

describe('BackendHealthService', () => {
  let service: BackendHealthService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(BackendHealthService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
