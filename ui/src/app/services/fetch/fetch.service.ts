import { HttpClient, HttpResponse } from '@angular/common/http'
import { Injectable } from '@angular/core'
import IDbConfig from 'src/app/model/db-config'
import ISession, { ISaveSessionPayload } from '../../model/session'
import IUpdateTable, { IReviewUpdateTable } from '../../model/update-table'
import IConv, {
  ICreateIndex,
  IForeignKey,
  IInterleaveStatus,
  IPrimaryKey,
  ISessionSummary,
} from '../../model/conv'
import IDumpConfig, { IConvertFromDumpRequest } from '../../model/dump-config'
import ISessionConfig from '../../model/session-config'
import ISpannerConfig from '../../model/spanner-config'
import IMigrationDetails, { IGeneratedResources, IProgress } from 'src/app/model/migrate'
import IConnectionProfile, { ICreateConnectionProfile } from 'src/app/model/profile'
import IRule from 'src/app/model/rule'

@Injectable({
  providedIn: 'root',
})
export class FetchService {
  private url: string = window.location.origin
  constructor(private http: HttpClient) {}

  connectTodb(payload: IDbConfig, dialect: string) {
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
        Dialect: dialect,
      },
      { observe: 'response' }
    )
  }

  getLastSessionDetails() {
    return this.http.get<IConv>(`${this.url}/GetLatestSessionDetails`)
  }
  getSchemaConversionFromDirectConnect() {
    return this.http.get<IConv>(`${this.url}/convert/infoschema`)
  }

  getSchemaConversionFromDump(payload: IConvertFromDumpRequest) {
    return this.http.post<IConv>(`${this.url}/convert/dump`, payload)
  }

  setSourceDBDetailsForDump(payload: IDumpConfig) {
    return this.http.post(`${this.url}/SetSourceDBDetailsForDump`, payload)
  }

  setSourceDBDetailsForDirectConnect(payload: IDbConfig) {
    const { dbEngine, hostName, port, dbName, userName, password } = payload
    return this.http.post(`${this.url}/SetSourceDBDetailsForDirectConnect`, {
      Driver: dbEngine,
      Host: hostName,
      Port: port,
      Database: dbName,
      User: userName,
      Password: password,
    })
  }

  getSchemaConversionFromSessionFile(payload: ISessionConfig) {
    return this.http.post<IConv>(`${this.url}/convert/session`, payload)
  }

  getConversionRate() {
    return this.http.get<Record<string, string>>(`${this.url}/conversion`)
  }

  getConnectionProfiles(isSource: boolean) {
    return this.http.get<IConnectionProfile[]>(
      `${this.url}/GetConnectionProfiles?source=${isSource}`
    )
  }

  getGeneratedResources() {
    return this.http.get<IGeneratedResources>(`${this.url}/GetGeneratedResources`)
  }

  getStaticIps() {
    return this.http.get<string[]>(`${this.url}/GetStaticIps`)
  }

  createConnectionProfile(payload: ICreateConnectionProfile) {
    return this.http.post(`${this.url}/CreateConnectionProfile`, payload)
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

  reviewTableUpdate(tableName: string, data: IUpdateTable): any {
    return this.http.post<HttpResponse<IReviewUpdateTable>>(
      `${this.url}/typemap/reviewTableSchema?table=${tableName}`,
      data
    )
  }

  updateTable(tableName: string, data: IUpdateTable): any {
    return this.http.post<HttpResponse<IConv>>(`${this.url}/typemap/table?table=${tableName}`, data)
  }

  removeInterleave(tableId: string) {
    return this.http.post<HttpResponse<IConv>>(`${this.url}/removeParent?tableId=${tableId}`, {})
  }

  restoreTable(tableId: string) {
    return this.http.post<HttpResponse<IConv>>(`${this.url}/restore/table?tableId=${tableId}`, {})
  }
  dropTable(tableId: string) {
    return this.http.post<HttpResponse<IConv>>(`${this.url}/drop/table?tableId=${tableId}`, {})
  }

  updatePk(pkObj: IPrimaryKey) {
    return this.http.post<HttpResponse<IConv>>(`${this.url}/primaryKey`, pkObj)
  }

  updateFk(tableId: string, payload: IForeignKey[]): any {
    return this.http.post<HttpResponse<IConv>>(`${this.url}/update/fks?table=${tableId}`, payload)
  }

  removeFk(tableName: string, fkName: string): any {
    return this.http.post<HttpResponse<IConv>>(`${this.url}/drop/fk?table=${tableName}`, {
      Name: fkName,
    })
  }

  getSessions() {
    return this.http.get<ISession[]>(`${this.url}/GetSessions`)
  }

  getConvForSession(versionId: string) {
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

  getIsOffline() {
    return this.http.get<boolean>(`${this.url}/IsOffline`)
  }

  updateIndex(tableName: string, payload: ICreateIndex[]) {
    return this.http.post<IConv>(`${this.url}/update/indexes?table=${tableName}`, payload)
  }

  dropIndex(tableName: string, indexName: string) {
    return this.http.post<IConv>(`${this.url}/drop/secondaryindex?table=${tableName}`, {
      Name: indexName,
    })
  }

  restoreIndex(tableId: string, indexId: string) {
    return this.http.post<HttpResponse<IConv>>(
      `${this.url}/restore/secondaryIndex?tableId=${tableId}&indexId=${indexId}`,
      {}
    )
  }

  getInterleaveStatus(tableName: string) {
    return this.http.get<IInterleaveStatus>(`${this.url}/setparent?table=${tableName}`)
  }

  setInterleave(tableName: string) {
    return this.http.get(`${this.url}/setparent?table=${tableName}&update=true`)
  }

  getSourceDestinationSummary() {
    return this.http.get<ISessionSummary>(`${this.url}/GetSourceDestinationSummary`)
  }

  migrate(payload: IMigrationDetails) {
    return this.http.post(`${this.url}/Migrate`, payload)
  }
  getProgress() {
    return this.http.get<IProgress>(`${this.url}/GetProgress`)
  }
  uploadFile(payload: FormData) {
    return this.http.post(`${this.url}/uploadFile`, payload)
  }
  cleanUpStreamingJobs() {
    return this.http.post(`${this.url}/CleanUpStreamingJobs`, {})
  }

  applyRule(payload: IRule) {
    return this.http.post(`${this.url}/applyrule`, payload)
  }

  dropRule(ruleId: string) {
    return this.http.post(`${this.url}/dropRule?id=${ruleId}`, {})
  }

  getStandardTypeToPGSQLTypemap() {
    return this.http.get<Map<string,string>>(`${this.url}/typemap/GetStandardTypeToPGSQLTypemap`)
  }
  getPGSQLToStandardTypeTypemap() {
    return this.http.get<Map<string,string>>(`${this.url}/typemap/GetPGSQLToStandardTypeTypemap`)
  }
}
