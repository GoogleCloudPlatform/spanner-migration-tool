import { Component, Inject, OnInit } from '@angular/core';
import { FormControl, Validators } from '@angular/forms';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material/dialog';
import { ITableState } from 'src/app/model/migrate';

@Component({
  selector: 'app-bulk-drop-restore-table-dialog',
  templateUrl: './bulk-drop-restore-table-dialog.component.html',
  styleUrls: ['./bulk-drop-restore-table-dialog.component.scss']
})
export class BulkDropRestoreTableDialogComponent implements OnInit {

  confirmationInput: FormControl
  eligibleTables: string[] = [];
  ineligibleTables: string[] = [];
  constructor(
    @Inject(MAT_DIALOG_DATA) public data: { tables: ITableState[], operation: string },
    private dialogRef: MatDialogRef<BulkDropRestoreTableDialogComponent>) {
    let regexPattern = ''
    if (this.data.operation == 'SKIP') {
      regexPattern = 'SKIP'
      this.data.tables.forEach((tableWithState) => {
        if (!tableWithState.isDeleted) {
          this.eligibleTables.push(tableWithState.TableName)
        }
        else {
          this.ineligibleTables.push(tableWithState.TableName)
        }
      })
    } else {
      regexPattern = 'RESTORE'
      this.data.tables.forEach((tableWithState) => {
        if (tableWithState.isDeleted) {
          this.eligibleTables.push(tableWithState.TableName)
        }
        else {
          this.ineligibleTables.push(tableWithState.TableName)
        }
      })
    }
    ; (this.confirmationInput = new FormControl('', [
      Validators.required,
      Validators.pattern(regexPattern),
    ])),
      (dialogRef.disableClose = true)
  }

  confirm() {
    this.dialogRef.close(this.data.operation)
  }

  ngOnInit(): void { }
}
