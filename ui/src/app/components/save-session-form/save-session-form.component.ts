import { Component, OnInit } from '@angular/core'
import { FormControl, FormGroup } from '@angular/forms'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { ISaveSessionPayload } from 'src/app/model/Session'
import { DataService } from 'src/app/services/data/data.service'
import { SnackbarService } from '../../services/snackbar/snackbar.service'

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
    private snack: SnackbarService
  ) {
    this.saveSessionFrom = new FormGroup({
      SessionName: new FormControl(''),
      EditorName: new FormControl(''),
      DatabaseType: new FormControl('mysql'),
      DatabaseName: new FormControl(''),
      Notes: new FormControl(''),
      Tags: new FormControl(''),
    })
  }

  saveSession() {
    let formValue = this.saveSessionFrom.value
    let payload: ISaveSessionPayload = {
      SessionName: formValue.SessionName,
      EditorName: formValue.EditorName,
      DatabaseType: formValue.DatabaseType,
      DatabaseName: formValue.DatabaseName,
      Notes: formValue.Notes.split('\n'),
      Tags: formValue.Tags.split(','),
    }

    console.log(payload)
    this.fetch.saveSession(payload).subscribe({
      next: (res: any) => {
        this.data.getAllSessions()
        this.snack.openSnackBar('Session save successfully', 'close', 5000)
      },
      error: (err: any) => {
        console.log(err)
        this.snack.openSnackBar(err.message, 'close', 5000)
      },
    })
  }

  ngOnInit(): void {}
}
