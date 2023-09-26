import { HttpClientModule } from '@angular/common/http';
import { TestBed } from '@angular/core/testing';
import { MatSnackBarModule } from '@angular/material/snack-bar';

import { ConversionService } from './conversion.service';

describe('ConversionService', () => {
  let service: ConversionService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [HttpClientModule],
    });
    service = TestBed.inject(ConversionService);
  });

  it('should be created', () => {
    expect(service).toBeTruthy();
  });
});
