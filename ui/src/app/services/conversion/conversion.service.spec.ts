import { HttpClientModule } from '@angular/common/http';
import { TestBed } from '@angular/core/testing';
import { MatSnackBarModule } from '@angular/material/snack-bar';
import IConv from '../../model/conv'

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

  it('getSpannerSequenceNameFromId', () => {
    let conv: IConv = {} as IConv 
    conv.SpSequences ={
      "s1": {
        Name: "Sequence1",
        Id: "s1",
        SequenceKind: "BIT REVERSED POSITIVE"
      },
      "s2": {
        Name: "Sequence2",
        Id: "s2",
        SequenceKind: "BIT REVERSED POSITIVE"
      },
    }
    const id = service.getSpannerSequenceNameFromId("s1", conv)
    expect(id).toEqual("Sequence1");
  });
});
