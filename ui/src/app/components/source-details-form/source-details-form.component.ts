import { Component, OnInit } from '@angular/core';
import { FormGroup, FormControl, Validators } from '@angular/forms';
import { InputType } from 'src/app/app.constants';
import IDumpConfig from 'src/app/model/dump-config';

@Component({
  selector: 'app-source-details-form',
  templateUrl: './source-details-form.component.html',
  styleUrls: ['./source-details-form.component.scss']
})
export class SourceDetailsFormComponent implements OnInit {
  inputOptions = [
    { value: InputType.DumpFile, display: 'Connect via dump file' },
    { value: InputType.DirectConnect, display: 'Connect via direct connection' },
  ]
  selectedOption: string = InputType.DumpFile
  constructor() { }
  connectForm = new FormGroup({
    dbEngine: new FormControl('mysqldump', [Validators.required]),
    filePath: new FormControl('', [Validators.required]),
  })
  dbEngineList = [
    { value: 'mysqldump', displayName: 'MYSQL' },
    { value: 'pg_dump', displayName: 'PostgreSQL' },
  ]

  ngOnInit(): void {
  }
  setSourceProfile() {
    const { dbEngine, filePath } = this.connectForm.value
    const payload: IDumpConfig = {
      Driver: dbEngine,
      Path: filePath,
    }
    // call the API to set source profile
  }

  onItemChange(optionValue: string) {
    this.selectedOption = optionValue
  }
}
