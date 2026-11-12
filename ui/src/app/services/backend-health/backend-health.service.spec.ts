import { provideHttpClient, withInterceptorsFromDi } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { TestBed } from '@angular/core/testing';
import { MatDialogModule } from '@angular/material/dialog';

import { BackendHealthService } from './backend-health.service';

describe('BackendHealthService', () => {
  let service: BackendHealthService;

  beforeEach(() => {
    TestBed.configureTestingModule({
    imports: [MatDialogModule],
    providers: [provideHttpClient(withInterceptorsFromDi()), provideHttpClientTesting()]
});
    service = TestBed.inject(BackendHealthService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
