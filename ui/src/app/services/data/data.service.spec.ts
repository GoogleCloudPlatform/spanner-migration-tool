import { TestBed } from '@angular/core/testing'
import { provideHttpClient, withInterceptorsFromDi } from '@angular/common/http'
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { DataService } from './data.service'
import { FetchService } from '../fetch/fetch.service'
import { SnackbarService } from '../snackbar/snackbar.service'
import { of } from 'rxjs'
import { createMockIConv } from 'src/mocks/conv'
import mockSpannerConfig from 'src/mocks/spannerConfig'

describe('DataService', () => {
    let service: DataService
    let snackbarService: SnackbarService;
    let fetchService: FetchService;
    let snackbarServiceSpy: { openSnackBar: ReturnType<typeof vi.fn> };
    let fetchServiceSpy: {
        getSpannerConfig: ReturnType<typeof vi.fn>;
        getIsOffline: ReturnType<typeof vi.fn>;
        getLastSessionDetails: ReturnType<typeof vi.fn>;
    };

    beforeEach(() => {
        snackbarServiceSpy = {
            openSnackBar: vi.fn(),
        };
        fetchServiceSpy = {
            getSpannerConfig: vi.fn().mockReturnValue(of(mockSpannerConfig)),
            getIsOffline: vi.fn().mockReturnValue(of(false)),
            getLastSessionDetails: vi.fn().mockReturnValue(of(createMockIConv())),
        };

        TestBed.configureTestingModule({
            imports: [],
            providers: [
                DataService,
                { provide: SnackbarService, useValue: snackbarServiceSpy },
                { provide: FetchService, useValue: fetchServiceSpy },
                provideHttpClient(withInterceptorsFromDi()), provideHttpClientTesting(),
            ]
        })
        snackbarService = TestBed.inject(SnackbarService);
        fetchService = TestBed.inject(FetchService);
        service = TestBed.inject(DataService)
    })

    it('should be created', () => {
        expect(service).toBeTruthy()
    })
})
