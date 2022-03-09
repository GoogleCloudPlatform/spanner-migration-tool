import { Component, OnInit, Inject } from '@angular/core'
import { MAT_DIALOG_DATA } from '@angular/material/dialog'
interface IDialogProps {
  message: string
  type: 'warning' | 'error' | 'success'
}
@Component({
  selector: 'app-infodialog',
  templateUrl: './infodialog.component.html',
  styleUrls: ['./infodialog.component.scss'],
})
export class InfodialogComponent implements OnInit {
  constructor(@Inject(MAT_DIALOG_DATA) public data: IDialogProps) {}

  ngOnInit(): void {
    console.log(this.data)
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
