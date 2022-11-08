import { TestBed } from '@angular/core/testing';

import { TableUpdatePubSubService } from './table-update-pub-sub.service';

describe('TableUpdatePubSubService', () => {
  let service: TableUpdatePubSubService;

  beforeEach(() => {
    TestBed.configureTestingModule({});
    service = TestBed.inject(TableUpdatePubSubService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
