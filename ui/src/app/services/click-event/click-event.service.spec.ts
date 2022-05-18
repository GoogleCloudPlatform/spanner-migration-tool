import { TestBed } from '@angular/core/testing';

import { ClickEventService } from './click-event.service';

describe('ClickEventService', () => {
  let service: ClickEventService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(ClickEventService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
