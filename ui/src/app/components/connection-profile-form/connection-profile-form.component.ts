import { Component, OnInit } from '@angular/core';
import { FetchService } from 'src/app/services/fetch/fetch.service';

@Component({
  selector: 'app-connection-profile-form',
  templateUrl: './connection-profile-form.component.html',
  styleUrls: ['./connection-profile-form.component.scss']
})
export class ConnectionProfileFormComponent implements OnInit {

  constructor(private fetch: FetchService,
    ) { }

  ngOnInit(): void {
    this.fetch.getRegions().subscribe()
  }

}
