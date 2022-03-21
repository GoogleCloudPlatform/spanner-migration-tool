import { HttpClient, HttpResponse } from '@angular/common/http'
import { Injectable } from '@angular/core'
import { first, tap } from 'rxjs'
import IDbConfig from 'src/app/model/DbConfig'
import ISession from 'src/app/model/Session'
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

  getSchemaConversionFromDump(payload: IDumpConfig) {
    return this.http.post<IConv>(`${this.url}/convert/dump`, payload)
  }

  getSchemaConversionFromSessionFile(payload: ISessionConfig) {
    return this.http.post<IConv>(`${this.url}/session/resume`, payload)
  }

  getConversionRate() {
    return this.http.get<Record<string, string>>(`${this.url}/conversion`).pipe(tap(console.log))
  }

  getSummary() {
    return this.http.get(`${this.url}/summary`).pipe(tap(console.log))
  }

  getDdl() {
    return this.http.get(`${this.url}/ddl`).pipe(tap(console.log))
  }

  getTypeMap() {
    return this.http.get(`${this.url}/typemap`).pipe(tap(console.log))
  }

  updateTable(tableName: string, data: IUpdateTable): any {
    return this.http.post<HttpResponse<IConv>>(`${this.url}/typemap/table?table=${tableName}`, data)
  }

  getSessions() {
    return this.http.get<ISession[]>(`${this.url}/GetSessions`)
  }

  getConvForAsession(versionId:string){
    return this.http.get(`${this.url}/GetSession/{versionId}`, {
      responseType: 'blob',
    })
  }
}
