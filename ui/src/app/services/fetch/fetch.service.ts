import { HttpClient, HttpResponse } from '@angular/common/http'
import { Injectable } from '@angular/core'
import IDbConfig from 'src/app/model/DbConfig'
import IUpdateTable from 'src/app/model/updateTable'
import IConv from '../../model/Conv'
import IDumpConfig from '../../model/DumpConfig'
import ISessionConfig from '../../model/SessionConfig'

@Injectable({
  providedIn: 'root',
})
export class FetchService {
  private url: string = 'http://localhost:8080'
  constructor(private http: HttpClient) {}

  connectTodb(payload: IDbConfig) {
    const { dbEngine, hostName, port, dbName, userName, password } = payload
    return this.http.post<HttpResponse<null>>(
      `${this.url}/connect`,
      {
        Driver: dbEngine,
        Host: hostName,
        Port: port,
        Database: dbName,
        User: userName,
        Password: password,
      },
      { observe: 'response' }
    )
  }

  getSchemaConversionFromDirectConnect() {
    return this.http.get<IConv>(`${this.url}/convert/infoschema`)
  }
  getConversionRate() {
    return this.http.get<Record<string, string>>(`${this.url}/conversion`)
  }

  getSchemaConversionFromDump(payload: IDumpConfig) {
    return this.http.post<IConv>(`${this.url}/convert/dump`, payload)
  }

  getSchemaConversionFromSessionFile(payload: ISessionConfig) {
    return this.http.post<IConv>(`${this.url}/session/resume`, payload)
  }

  getSummary() {
    return this.http.get(`${this.url}/summary`)
  }

  getTypeMap() {
    return this.http.get(`${this.url}/typemap`)
  }

  updateTable(tableName: string, data: IUpdateTable): any {
    return this.http.post<HttpResponse<IConv>>(`${this.url}/typemap/table?table=${tableName}`, data)
  }
}
