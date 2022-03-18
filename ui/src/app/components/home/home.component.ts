import { Component, OnInit } from '@angular/core'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import ISession from '../../model/Session'
import { DataService } from 'src/app/services/data/data.service'

@Component({
  selector: 'app-home',
  templateUrl: './home.component.html',
  styleUrls: ['./home.component.scss'],
})
export class HomeComponent implements OnInit {
  sessions: ISession[] = []
  constructor(private data: DataService) {}

  ngOnInit(): void {
    this.data.sessions.subscribe((sessions: ISession[]) => {
      console.log(sessions)
      this.sessions = sessions.map((obj) => {
        //obj['action'] = ''
        return obj
      })
    })
  }
}
