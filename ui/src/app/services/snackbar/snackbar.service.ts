import { Injectable } from '@angular/core'
import { MatSnackBar } from '@angular/material/snack-bar'

@Injectable({
  providedIn: 'root',
})
export class SnackbarService {
  constructor(private snackBar: MatSnackBar) {}

  openSnackBar(message: string, action: string, durationInSeconds?: number) {
    if (!durationInSeconds) durationInSeconds = 10
    else this.snackBar.open(message, action, { duration: durationInSeconds * 1000 })
  }
}
