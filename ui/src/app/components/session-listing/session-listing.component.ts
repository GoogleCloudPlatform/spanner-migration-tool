import { Component, Input, OnInit, SimpleChanges } from '@angular/core'
import ISession from '../../model/Session'

@Component({
  selector: 'app-session-listing',
  templateUrl: './session-listing.component.html',
  styleUrls: ['./session-listing.component.scss'],
})
export class SessionListingComponent implements OnInit {
  @Input() sessions: ISession[] = []
  displayedColumns = [
    'sessionname',
    // 'versionid',
    'databasetype',
    'databasename',
    'editorname',
    'action',
  ]
  dataSource = this.sessions

  constructor() {}

  ngOnInit(): void {}

  ngOnChanges(changes: SimpleChanges): void {
    let newSessions = changes?.['sessions'].currentValue
    this.dataSource = newSessions
    console.log(this.dataSource)
  }
}
