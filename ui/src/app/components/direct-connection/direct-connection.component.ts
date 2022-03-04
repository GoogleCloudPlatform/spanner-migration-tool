import { Component, OnInit } from '@angular/core'
import { FormControl, FormGroup } from '@angular/forms'

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

  constructor() {}

  ngOnInit(): void {}

  connectToDb() {
    // const { dbEngine, hostName, port, userName, password, dbName } = this.connectForm.value
    // this._loader.startLoader()
    // window.scrollTo(0, 0)
    // this._fetch.connectTodb(dbEngine, hostName, port, userName, password, dbName).subscribe({
    //   next: (res) => {
    //     if (res.status == 200) {
    //       localStorage.setItem(
    //         'connectionConfig',
    //         JSON.stringify({ dbEngine, hostName, port, userName, password, dbName })
    //       )
    //     }
    //     this.router.navigate(['/workspace'])
    //   },
    //   error: (e) => console.log(e),
    //   complete: () => this._loader.stopLoader(),
    // })
  }
}
