import { TestBed } from '@angular/core/testing'
import { HttpClientModule } from '@angular/common/http'
import { DataService } from './data.service'
import { FetchService } from '../fetch/fetch.service'
import { SnackbarService } from '../snackbar/snackbar.service'
import { of } from 'rxjs'
import mockIConv from 'src/mocks/conv'
import mockSpannerConfig from 'src/mocks/spannerConfig'

describe('DataService', () => {
    let service: DataService
    let snackbarService: SnackbarService;
    let fetchService: FetchService;

    // Create spy objects for dependencies
    const snackbarServiceSpy = jasmine.createSpyObj('SnackbarService', ['openSnackBar']);
    const fetchServiceSpy = jasmine.createSpyObj('FetchService', ['getSpannerConfig', 'getIsOffline', 'getLastSessionDetails']);

    beforeEach(() => {
        TestBed.configureTestingModule({
            imports: [HttpClientModule],
            providers: [
                DataService,
                { provide: SnackbarService, useValue: snackbarServiceSpy },
                { provide: FetchService, useValue: fetchServiceSpy },
            ],
        })
        fetchServiceSpy.getLastSessionDetails.and.returnValue(of(mockIConv));
        fetchServiceSpy.getSpannerConfig.and.returnValue(of(mockSpannerConfig));
        fetchServiceSpy.getIsOffline.and.returnValue(of(false));
        snackbarService = TestBed.inject(SnackbarService);
        fetchService = TestBed.inject(FetchService) as jasmine.SpyObj<FetchService>;
        service = TestBed.inject(DataService)
    })

    it('should be created', () => {
        expect(service).toBeTruthy()
    })
})