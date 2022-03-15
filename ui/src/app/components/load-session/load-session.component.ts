import { Component, OnInit } from '@angular/core'
import { FormControl, FormGroup } from '@angular/forms'
import IDumpConfig from 'src/app/model/DumpConfig'
import { DataService } from 'src/app/services/data/data.service'
import ISessionConfig from '../../model/SessionConfig'
import { Router } from '@angular/router'
import { InputType, StorageKeys } from 'src/app/app.constants'

@Component({
  selector: 'app-load-session',
  templateUrl: './load-session.component.html',
  styleUrls: ['./load-session.component.scss'],
})
export class LoadSessionComponent implements OnInit {
  constructor(private data: DataService, private router: Router) {}

  connectForm = new FormGroup({
    dbEngine: new FormControl('sqlserver'),
    filePath: new FormControl('harbour_bridge_output/BikeStores/BikeStores.session.json'),
  })

  ngOnInit(): void {}

  convertFromSessionFile() {
    this.data.resetStore()
    const { dbEngine, filePath } = this.connectForm.value
    const payload: ISessionConfig = {
      driver: dbEngine,
      filePath: filePath,
    }
    this.data.getSchemaConversionFromSession(payload)
    this.data.conv.subscribe((res) => {
      console.log(res)
      localStorage.setItem(StorageKeys.Config, JSON.stringify(payload))
      localStorage.setItem(StorageKeys.Type, InputType.SessionFile)
      this.router.navigate(['/workspace'])
    })
  }
}
