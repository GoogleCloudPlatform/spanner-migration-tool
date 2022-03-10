import { TestBed } from '@angular/core/testing'
import { MatSnackBarModule } from '@angular/material/snack-bar'
import { SnackbarService } from './snackbar.service'

describe('SnackbarService', () => {
  let service: SnackbarService

  beforeEach(() => {
    TestBed.configureTestingModule({
      imports: [MatSnackBarModule],
    })
    service = TestBed.inject(SnackbarService)
  })

  it('should be created', () => {
    expect(service).toBeTruthy()
  })
})
