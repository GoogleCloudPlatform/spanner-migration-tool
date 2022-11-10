import { Component, OnInit } from '@angular/core'
import { FormControl, FormGroup, Validators } from '@angular/forms'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { ISaveSessionPayload } from 'src/app/model/session'
import { DataService } from 'src/app/services/data/data.service'
import { SnackbarService } from '../../services/snackbar/snackbar.service'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'

@Component({
  selector: 'app-sidenav-save-session',
  templateUrl: './sidenav-save-session.component.html',
  styleUrls: ['./sidenav-save-session.component.scss'],
})
export class SidenavSaveSessionComponent implements OnInit {
  errMessage: string = ''
  constructor(
    private fetch: FetchService,
    private data: DataService,
    private snack: SnackbarService,
    private sidenav: SidenavService
  ) {}
  saveSessionForm: FormGroup = new FormGroup({
    SessionName: new FormControl('', [
      Validators.required,
      Validators.pattern('^[a-zA-Z].{0,59}$'),
    ]),
    EditorName: new FormControl('', [Validators.pattern('^[a-zA-Z].{0,59}$')]),
    DatabaseName: new FormControl('', [
      Validators.required,
      Validators.pattern('^[a-zA-Z].{0,59}$'),
    ]),
    Notes: new FormControl(''),
  })

  saveSession() {
    let formValue = this.saveSessionForm.value
    let payload: ISaveSessionPayload = {
      SessionName: formValue.SessionName.trim(),
      EditorName: formValue.EditorName.trim(),
      DatabaseName: formValue.DatabaseName.trim(),
      Notes: formValue.Notes?.trim() === '' ? undefined : formValue.Notes?.split('\n'),
    }

    this.fetch.saveSession(payload).subscribe({
      next: (res: any) => {
        this.data.getAllSessions()
        this.snack.openSnackBar('Session saved successfully', 'Close', 5)
      },
      error: (err: any) => {
        this.snack.openSnackBar(err.error, 'Close')
      },
    })
    this.saveSessionForm.reset()
    this.saveSessionForm.markAsUntouched()
    this.closeSidenav()
  }

  ngOnInit(): void {}
  closeSidenav(): void {
    this.sidenav.closeSidenav()
  }
}
