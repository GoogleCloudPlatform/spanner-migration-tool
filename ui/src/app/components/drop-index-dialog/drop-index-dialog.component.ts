import { Component, Inject, OnInit } from '@angular/core'
import { FormControl, Validators } from '@angular/forms'
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog'

@Component({
  selector: 'app-drop-index-dialog',
  templateUrl: './drop-index-dialog.component.html',
  styleUrls: ['./drop-index-dialog.component.scss'],
})
export class DropIndexDialogComponent implements OnInit {
  confirmationInput: FormControl
  constructor(
    @Inject(MAT_DIALOG_DATA) public data: string,
    private dialogRef: MatDialogRef<DropIndexDialogComponent>
  ) {
    ;(this.confirmationInput = new FormControl('', [
      Validators.required,
      Validators.pattern(`^${data}$`),
    ])),
      (dialogRef.disableClose = true)
  }

  deleteIndex() {
    this.dialogRef.close('delete')
  }

  ngOnInit(): void {}
}
