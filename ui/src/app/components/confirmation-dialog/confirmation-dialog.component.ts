import { Component, Inject, OnInit } from '@angular/core'
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog'

interface IConfirmDialogProps {
  title: string
  message: string
}

@Component({
  selector: 'app-confirmation-dialog',
  templateUrl: './confirmation-dialog.component.html',
  styleUrls: ['./confirmation-dialog.component.scss'],
})
export class ConfirmationDialogComponent implements OnInit {
  constructor(
    public dialogRef: MatDialogRef<ConfirmationDialogComponent>,
    @Inject(MAT_DIALOG_DATA) public data: IConfirmDialogProps
  ) {}

  onConfirm(): void {
    // Close the dialog, return true
    this.dialogRef.close(true)
  }

  onDismiss(): void {
    // Close the dialog, return false
    this.dialogRef.close(false)
  }

  ngOnInit(): void {}
}
