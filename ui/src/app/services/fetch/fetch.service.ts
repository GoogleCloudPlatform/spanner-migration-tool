import { HttpClient, HttpResponse } from '@angular/common/http'
import { Injectable } from '@angular/core'
import IDbConfig from 'src/app/model/db-config'
import ISession, { ISaveSessionPayload } from '../../model/session'
import IUpdateTable from '../../model/update-table'
import IConv, { ICreateIndex, IInterleaveStatus, IPrimaryKey } from '../../model/conv'
import IDumpConfig from '../../model/dump-config'
import ISessionConfig from '../../model/session-config'
import ISpannerConfig from '../../model/spanner-config'

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
    return this.http.get<Record<string, string>>(`${this.url}/conversion`)
  }

  getSummary() {
    return this.http.get(`${this.url}/summary`)
  }

  getDdl() {
    return this.http.get(`${this.url}/ddl`)
  }

  getTypeMap() {
    return this.http.get(`${this.url}/typemap`)
  }

  updateTable(tableName: string, data: IUpdateTable): any {
    return this.http.post<HttpResponse<IConv>>(`${this.url}/typemap/table?table=${tableName}`, data)
  }

  dropTable(tableName: string) {
    return this.http.put<HttpResponse<IConv>>(`${this.url}/drop/table?table=${tableName}`, {})
  }

  updatePk(pkObj: IPrimaryKey) {
    return this.http.post<HttpResponse<IConv>>(`${this.url}/primaryKey`, pkObj)
  }

  updateFk(tableName: string, payload: Record<string, string>): any {
    return this.http.post<HttpResponse<IConv>>(`${this.url}/rename/fks?table=${tableName}`, payload)
  }

  removeFk(tableName: string, fkName: string): any {
    return this.http.post<HttpResponse<IConv>>(`${this.url}/drop/fk?table=${tableName}`, {
      Name: fkName,
    })
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

  addIndex(tableName: string, payload: ICreateIndex[]) {
    return this.http.post<IConv>(`${this.url}/add/indexes?table=${tableName}`, payload)
  }

  updateIndex(tableName: string, payload: ICreateIndex[]) {
    return this.http.post<IConv>(`${this.url}/update/indexes?table=${tableName}`, payload)
  }

  dropIndex(tableName: string, indexName: string) {
    return this.http.post<IConv>(`${this.url}/drop/secondaryindex?table=${tableName}`, {
      Name: indexName,
    })
  }

  getInterleaveStatus(tableName: string) {
    return this.http.get<IInterleaveStatus>(`${this.url}/setparent?table=${tableName}`)
  }

  setInterleave(tableName: string) {
    return this.http.get(`${this.url}/setparent?table=${tableName}&update=true`)
  }
}
