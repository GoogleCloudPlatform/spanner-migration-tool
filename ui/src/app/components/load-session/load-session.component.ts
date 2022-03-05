import { Component, OnInit } from '@angular/core'
import { FormControl, FormGroup } from '@angular/forms'

@Component({
  selector: 'app-load-session',
  templateUrl: './load-session.component.html',
  styleUrls: ['./load-session.component.scss'],
})
export class LoadSessionComponent implements OnInit {
  
  constructor() {}

  connectForm = new FormGroup({
    dbEngine: new FormControl('sqlserver'),
    filePath: new FormControl(' '),
  })

  ngOnInit(): void {}
}
