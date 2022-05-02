import { Component, OnInit } from '@angular/core'
import { FormControl, FormGroup } from '@angular/forms'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { ISaveSessionPayload } from 'src/app/model/Session'
import { DataService } from 'src/app/services/data/data.service'
import { SnackbarService } from '../../services/snackbar/snackbar.service'
import { MatDialogRef } from '@angular/material/dialog'

@Component({
  selector: 'app-save-session-form',
  templateUrl: './save-session-form.component.html',
  styleUrls: ['./save-session-form.component.scss'],
})
export class SaveSessionFormComponent implements OnInit {
  errMessage: string = ''
  saveSessionFrom: FormGroup
  constructor(
    private fetch: FetchService,
    private data: DataService,
    private snack: SnackbarService,
    private dialogRef: MatDialogRef<SaveSessionFormComponent>
  ) {
    this.saveSessionFrom = new FormGroup({
      SessionName: new FormControl(''),
      EditorName: new FormControl(''),
      DatabaseName: new FormControl(''),
      Notes: new FormControl(''),
    })
  }

  saveSession() {
    let formValue = this.saveSessionFrom.value
    let payload: ISaveSessionPayload = {
      SessionName: formValue.SessionName,
      EditorName: formValue.EditorName,
      DatabaseName: formValue.DatabaseName,
      Notes: formValue.Notes.split('\n'),
    }

    this.fetch.saveSession(payload).subscribe({
      next: (res: any) => {
        this.data.getAllSessions()
        this.snack.openSnackBar('Session saved successfully', 'Close', 5000)
        this.dialogRef.close()
      },
      error: (err: any) => {
        this.snack.openSnackBar(err.error, 'Close', 5000)
      },
    })
  }

  ngOnInit(): void {}
}
