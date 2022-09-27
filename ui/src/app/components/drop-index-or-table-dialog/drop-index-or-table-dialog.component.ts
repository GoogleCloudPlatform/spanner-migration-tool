import { Component, Inject, OnInit } from '@angular/core'
import { FormControl, Validators } from '@angular/forms'
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog'
import { ObjectDetailNodeType } from 'src/app/app.constants'

@Component({
  selector: 'app-drop-index-dialog',
  templateUrl: './drop-index-or-table-dialog.component.html',
  styleUrls: ['./drop-index-or-table-dialog.component.scss'],
})
export class DropIndexOrTableDialogComponent implements OnInit {
  ObjectDetailNodeType = ObjectDetailNodeType
  confirmationInput: FormControl
  constructor(
    @Inject(MAT_DIALOG_DATA) public data: { name: string; type: string },
    private dialogRef: MatDialogRef<DropIndexOrTableDialogComponent>
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
