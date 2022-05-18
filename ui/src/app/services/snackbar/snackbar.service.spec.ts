import { TestBed } from '@angular/core/testing'
import { MatSnackBarModule } from '@angular/material/snack-bar'
import { SnackbarService } from './snackbar.service'
import { MatSnackBar } from '@angular/material/snack-bar'

describe('SnackbarService', () => {
  let snackBarService: SnackbarService
  let matSnackBarService: MatSnackBar

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [MatSnackBarModule],
      providers: [MatSnackBar, SnackbarService],
    })
    snackBarService = TestBed.inject(SnackbarService)
    matSnackBarService = TestBed.inject(MatSnackBar)
  })

  it('should be created', () => {
    expect(snackBarService).toBeTruthy()
  })

  it('should call the MatSnackBar open method on openSnackBar method call', () => {
    const matSnackBarSpy = spyOn(matSnackBarService, 'open').and.stub()
    snackBarService.openSnackBar('message', 'action')
    expect(matSnackBarSpy.calls.count()).toEqual(1)
  })
})
