import { Component, OnInit, Inject } from '@angular/core'
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog'
interface IDialogProps {
  message: string
  type: 'warning' | 'error' | 'success'
  title: string
}
@Component({
  selector: 'app-infodialog',
  templateUrl: './infodialog.component.html',
  styleUrls: ['./infodialog.component.scss'],
})
export class InfodialogComponent implements OnInit {
  constructor(
    public dialogRef: MatDialogRef<InfodialogComponent>,
    @Inject(MAT_DIALOG_DATA) public data: IDialogProps
  ) {
    if (data.title === undefined) {
      data.title = 'Update can not be saved'
    }
  }

  ngOnInit(): void {}

  onConfirm(): void {
    // Close the dialog, return true
    this.dialogRef.close(true)
  }

  onDismiss(): void {
    // Close the dialog, return false
    this.dialogRef.close(false)
  }

  getIconFromMessageType() {
    switch (this.data.type) {
      case 'warning':
        return 'warning'
      case 'error':
        return 'error'
      case 'success':
        return 'check_circle'
      default:
        return 'message'
    }
  }
}
