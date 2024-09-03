import { Component, Inject, OnInit } from '@angular/core'
import { FormControl, Validators } from '@angular/forms'
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog'
import { ObjectDetailNodeType } from 'src/app/app.constants'

@Component({
  selector: 'app-drop-index-dialog',
  templateUrl: './drop-object-detail-dialog.component.html',
  styleUrls: ['./drop-object-detail-dialog.component.scss'],
})
export class DropObjectDetailDialogComponent implements OnInit {
  ObjectDetailNodeType = ObjectDetailNodeType
  confirmationInput: FormControl
  constructor(
    @Inject(MAT_DIALOG_DATA) public data: { name: string; type: string },
    private dialogRef: MatDialogRef<DropObjectDetailDialogComponent>
  ) {
    ;(this.confirmationInput = new FormControl('', [
      Validators.required,
      Validators.pattern(`^${data.name}$`),
    ])),
      (dialogRef.disableClose = true)
  }

  delete() {
    this.dialogRef.close(this.data.type)
  }

  ngOnInit(): void {}
}
