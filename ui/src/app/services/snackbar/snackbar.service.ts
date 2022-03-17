import { Injectable } from '@angular/core'
import { MatSnackBar } from '@angular/material/snack-bar'

@Injectable({
  providedIn: 'root',
})
export class SnackbarService {
  constructor(private snackBar: MatSnackBar) {}

  openSnackBar(message: string, action: string, duration?: number) {
    if (duration === null) this.snackBar.open(message, action)
    else this.snackBar.open(message, action, { duration: duration })
  }
}
