import { Component, OnInit } from '@angular/core'
import { FormControl, FormGroup } from '@angular/forms'

@Component({
  selector: 'app-load-dump',
  templateUrl: './load-dump.component.html',
  styleUrls: ['./load-dump.component.scss'],
})
export class LoadDumpComponent implements OnInit {
  constructor() {}
  connectForm = new FormGroup({
    dbEngine: new FormControl('sqlserver'),
    filePath: new FormControl(' '),
  })
  ngOnInit(): void {}
}
