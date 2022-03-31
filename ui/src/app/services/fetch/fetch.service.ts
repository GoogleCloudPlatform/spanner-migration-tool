import { HttpClient, HttpResponse } from '@angular/common/http'
import { Injectable } from '@angular/core'
import { first, tap } from 'rxjs'
import IDbConfig from 'src/app/model/DbConfig'
import ISession, { ISaveSessionPayload } from 'src/app/model/Session'
import IUpdateTable from 'src/app/model/updateTable'
import IConv, { IGlobalType } from '../../model/Conv'
import IDumpConfig from '../../model/DumpConfig'
import ISessionConfig from '../../model/SessionConfig'
import ISpannerConfig from '../../model/SpannerConfig'

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
    return this.http.post<IConv>(`${this.url}/convert/session`, payload)
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

  getConvForAsession(versionId: string) {
    return this.http.get(`${this.url}/GetSession/${versionId}`, {
      responseType: 'blob',
    })
  }

  resumeSession(versionId: string) {
    return this.http.post<IConv>(`${this.url}/ResumeSession/${versionId}`, {})
  }

  saveSession(session: ISaveSessionPayload) {
    return this.http.post(`${this.url}/SaveRemoteSession`, session)
  }

  getSpannerConfig() {
    return this.http.get<ISpannerConfig>(`${this.url}/GetConfig`)
  }

  setSpannerConfig(payload: ISpannerConfig) {
    return this.http.post<ISpannerConfig>(`${this.url}/SetSpannerConfig`, payload)
  }

  InitiateSession() {
    return this.http.post<ISession>(`${this.url}/InitiateSession`, {})
  }

  updateGlobalType(types: Record<string, string>): any {
    return this.http.post<HttpResponse<IConv>>(`${this.url}/typemap/global`, types)
  }

  getIsOffline() {
    return this.http.get<boolean>(`${this.url}/IsOffline`)
  }
}
