import { HttpClient, HttpResponse } from '@angular/common/http'
import { Injectable } from '@angular/core'
import IDbConfig, { IDbConfigs } from 'src/app/model/db-config'
import ISession, { ISaveSessionPayload } from '../../model/session'
import IUpdateTable, { IAddColumn, IReviewUpdateTable } from '../../model/update-table'
import IConv, {
  ICreateIndex,
  IForeignKey,
  IInterleaveStatus,
  IPrimaryKey,
  ISessionSummary,
  ITableIdAndName,
} from '../../model/conv'
import IDumpConfig, { IConvertFromDumpRequest } from '../../model/dump-config'
import ISessionConfig from '../../model/session-config'
import ISpannerConfig from '../../model/spanner-config'
import IMigrationDetails, { IGeneratedResources, IProgress, ITables } from 'src/app/model/migrate'
import IConnectionProfile, { ICreateConnectionProfileV2, IDataflowConfig, IMigrationProfile } from 'src/app/model/profile'
import IRule from 'src/app/model/rule'
import IStructuredReport from 'src/app/model/structured-report'

@Injectable({
  providedIn: 'root',
})
export class FetchService {
  private url: string = window.location.origin
  constructor(private http: HttpClient) {}

  connectTodb(payload: IDbConfig, dialect: string) {
    const { dbEngine, isSharded, hostName, port, dbName, userName, password } = payload
    return this.http.post<HttpResponse<null>>(
      `${this.url}/connect`,
      {
        Driver: dbEngine,
        IsSharded: isSharded,
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

  setShardsSourceDBDetailsForBulk(payload: IDbConfigs) {
    const { dbConfigs, isRestoredSession } = payload
    let mappedDBConfig: Array<any> = []
    dbConfigs.forEach( (dbConfig) => {
      mappedDBConfig.push( {
        Driver: dbConfig.dbEngine,
        Host: dbConfig.hostName,
        Port: dbConfig.port,
        Database: dbConfig.dbName,
        User: dbConfig.userName,
        Password: dbConfig.password,
        DataShardId: dbConfig.shardId,
      })
    })
    return this.http.post(`${this.url}/SetShardsSourceDBDetailsForBulk`, {
      DbConfigs: mappedDBConfig,
      IsRestoredSession: isRestoredSession
    })
  }

  setShardsSourceDBDetailsForDataflow(payload: IMigrationProfile) {
    return this.http.post(`${this.url}/SetShardsSourceDBDetailsForDataflow`, {
      MigrationProfile: payload
    })
  }

  setDataflowDetailsForShardedMigrations(payload: IDataflowConfig) {
    return this.http.post(`${this.url}/SetDataflowDetailsForShardedMigrations`, {
      DataflowConfig: payload
    })
  }

  getSourceProfile() {
    return this.http.get<IMigrationProfile>(`${this.url}/GetSourceProfileConfig`)
  }

  getSchemaConversionFromSessionFile(payload: ISessionConfig) {
    return this.http.post<IConv>(`${this.url}/convert/session`, payload)
  }

  getDStructuredReport(){
    return this.http.get<IStructuredReport>(`${this.url}/downloadStructuredReport`)
  }

  getDTextReport(){
    return this.http.get<string>(`${this.url}/downloadTextReport`)
  }

  getDSpannerDDL(){
    return this.http.get<string>(`${this.url}/downloadDDL`)
  }

  getIssueDescription(){
    return this.http.get<{[key: string]: string}>(`${this.url}/issueDescription`)
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

  createConnectionProfile(payload: ICreateConnectionProfileV2) {
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

  getSpannerDefaultTypeMap() {
    return this.http.get(`${this.url}/spannerDefaultTypeMap`)
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

  restoreTables(payload: ITables) {
    return this.http.post(`${this.url}/restore/tables`, payload)
  }

  restoreTable(tableId: string) {
    return this.http.post<HttpResponse<IConv>>(`${this.url}/restore/table?table=${tableId}`, {})
  }
  dropTable(tableId: string) {
    return this.http.post<HttpResponse<IConv>>(`${this.url}/drop/table?table=${tableId}`, {})
  }
  
  dropTables(payload: ITables) {
    return this.http.post(`${this.url}/drop/tables`, payload)
  }

  updatePk(pkObj: IPrimaryKey) {
    return this.http.post<HttpResponse<IConv>>(`${this.url}/primaryKey`, pkObj)
  }

  updateFk(tableId: string, payload: IForeignKey[]): any {
    return this.http.post<HttpResponse<IConv>>(`${this.url}/update/fks?table=${tableId}`, payload)
  }

  addColumn(tableId: string,payload: IAddColumn) {
    return this.http.post(`${this.url}/AddColumn?table=${tableId}`, payload)
  }

  removeFk(tableId: string, fkId: string): any {
    return this.http.post<HttpResponse<IConv>>(`${this.url}/drop/fk?table=${tableId}`, {
      Id: fkId,
    })
  }

  getTableWithErrors() {
    return this.http.get<ITableIdAndName[]>(`${this.url}/GetTableWithErrors`)
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

  updateIndex(tableId: string, payload: ICreateIndex[]) {
    return this.http.post<IConv>(`${this.url}/update/indexes?table=${tableId}`, payload)
  }

  dropIndex(tableId: string, indexName: string) {
    return this.http.post<IConv>(`${this.url}/drop/secondaryindex?table=${tableId}`, {
      Id: indexName,
    })
  }

  restoreIndex(tableId: string, indexId: string) {
    return this.http.post<HttpResponse<IConv>>(
      `${this.url}/restore/secondaryIndex?tableId=${tableId}&indexId=${indexId}`,
      {}
    )
  }

  getInterleaveStatus(tableId: string) {
    return this.http.get<IInterleaveStatus>(`${this.url}/setparent?table=${tableId}&update=false`)
  }

  setInterleave(tableId: string) {
    return this.http.get(`${this.url}/setparent?table=${tableId}&update=true`)
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
  checkBackendHealth() {
    return this.http.get(`${this.url}/ping`)
  }
}
