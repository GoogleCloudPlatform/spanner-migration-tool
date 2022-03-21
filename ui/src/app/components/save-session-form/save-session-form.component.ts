import { Component, OnInit } from '@angular/core'
import { FormControl, FormGroup } from '@angular/forms'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { ISaveSessionPayload } from 'src/app/model/Session'

@Component({
  selector: 'app-save-session-form',
  templateUrl: './save-session-form.component.html',
  styleUrls: ['./save-session-form.component.scss'],
})
export class SaveSessionFormComponent implements OnInit {
  isLoading: boolean = false
  errMessage: string = ''
  saveSessionFrom: FormGroup
  constructor(private fetch: FetchService) {
    this.saveSessionFrom = new FormGroup({
      SessionName: new FormControl(''),
      EditorName: new FormControl(''),
      DatabaseType: new FormControl(''),
      DatabaseName: new FormControl(''),
      Notes: new FormControl(''),
      Tags: new FormControl(''),
    })
  }

  saveSession() {
    this.isLoading = true
    let formValue = this.saveSessionFrom.value

    console.log(formValue)

    let payload: ISaveSessionPayload = {
      SessionName: formValue.SessionName,
      EditorName: formValue.EditorName,
      DatabaseType: formValue.DatabaseType,
      DatabaseName: formValue.DatabaseName,
      Notes: formValue.Notes.split('/n'),
      Tags: formValue.Tags.split('/n'),
    }
    this.fetch.saveSession(payload).subscribe({
      next: (data: any) => {
        console.log(data)
      },
      error: (err: any) => {
        console.log(err)
        this.errMessage = err.message
      },
      complete: () => {
        this.isLoading = false
      },
    })
  }

  ngOnInit(): void {}
}
