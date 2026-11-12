import { TestBed } from '@angular/core/testing';
import { MatSnackBarModule, MatSnackBar } from '@angular/material/snack-bar';
import { SnackbarService } from './snackbar.service';

describe('SnackbarService', () => {
  let snackBarService: SnackbarService;
  let matSnackBarService: MatSnackBar;

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [MatSnackBarModule],
      providers: [SnackbarService],
    });

    snackBarService = TestBed.inject(SnackbarService);
    matSnackBarService = TestBed.inject(MatSnackBar);
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it('should be created', () => {
    expect(snackBarService).toBeTruthy();
  });

  it('should call the MatSnackBar open method on openSnackBar method call', () => {
    const matSnackBarSpy = vi.spyOn(matSnackBarService, 'open').mockImplementation(() => ({} as any));
    snackBarService.openSnackBar('message', 'action');
    expect(matSnackBarSpy).toHaveBeenCalledTimes(1);
    expect(matSnackBarSpy).toHaveBeenCalledWith('message', 'action', { duration: 10000 });
  });
});
