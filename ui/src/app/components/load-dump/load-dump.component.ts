import { Component, OnInit } from '@angular/core'
import { FormControl, FormGroup } from '@angular/forms'
import IDumpConfig from 'src/app/model/DumpConfig'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { DataService } from 'src/app/services/data/data.service'
import { Router } from '@angular/router'
import { InputType, StorageKeys } from 'src/app/app.constants'

@Component({
  selector: 'app-load-dump',
  templateUrl: './load-dump.component.html',
  styleUrls: ['./load-dump.component.scss'],
})
export class LoadDumpComponent implements OnInit {
  constructor(private data: DataService, private router: Router) {}
  connectForm = new FormGroup({
    dbEngine: new FormControl('mysqldump'),
    filePath: new FormControl('test_data/frontend/sakila.sql'),
  })
  ngOnInit(): void {}

  convertFromDump() {
    this.data.resetStore()
    const { dbEngine, filePath } = this.connectForm.value
    const payload: IDumpConfig = {
      Driver: dbEngine,
      Path: filePath,
    }
    this.data.getSchemaConversionFromDump(payload)
    this.data.conv.subscribe((res) => {
      localStorage.setItem(StorageKeys.Config, JSON.stringify(payload))
      localStorage.setItem(StorageKeys.Type, InputType.DumpFile)
      console.log(res)
      this.router.navigate(['/workspace'])
    })
  }
}
