import { TestBed } from '@angular/core/testing';

import { TargetDetailsService } from './target-details.service';

describe('TargetDetailsService', () => {
  let service: TargetDetailsService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(TargetDetailsService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
