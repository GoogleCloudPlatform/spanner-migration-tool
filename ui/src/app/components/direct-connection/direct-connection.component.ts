import { Component, OnInit } from '@angular/core'
import { FormControl, FormGroup } from '@angular/forms'
import { Router } from '@angular/router'
import IDbConfig from 'src/app/model/DbConfig'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { DataService } from 'src/app/services/data/data.service'

@Component({
  selector: 'app-direct-connection',
  templateUrl: './direct-connection.component.html',
  styleUrls: ['./direct-connection.component.scss'],
})
export class DirectConnectionComponent implements OnInit {
  connectForm = new FormGroup({
    dbEngine: new FormControl('sqlserver'),
    hostName: new FormControl('104.198.154.85'),
    port: new FormControl('1433'),
    userName: new FormControl('sa'),
    password: new FormControl('P@ssw0rd.1'),
    dbName: new FormControl('BikeStores'),
  })

  constructor(private router: Router, private fetch: FetchService, private data: DataService) {}

  ngOnInit(): void {}

  connectToDb() {
    const { dbEngine, hostName, port, userName, password, dbName } = this.connectForm.value
    const config: IDbConfig = { dbEngine, hostName, port, userName, password, dbName }
    this.fetch.connectTodb(config).subscribe({
      next: (res) => {
        if (res.status == 200) {
          localStorage.setItem(
            'connectionConfig',
            JSON.stringify({ dbEngine, hostName, port, userName, password, dbName })
          )
        }
        this.data.getSchemaConversionData()
        this.router.navigate(['/workspace'])
      },
      error: (e) => console.log(e),
    })
  }
}
