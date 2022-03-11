import { Injectable } from '@angular/core'
import { MatSnackBar, MatSnackBarRef } from '@angular/material/snack-bar'

@Injectable({
  providedIn: 'root',
})
export class SnackbarService {
  constructor(private snackBar: MatSnackBar) {}

  openSnackBar(message: string, action: string) {
    this.snackBar.open(message, action, { duration: 2000 })
  }
}
