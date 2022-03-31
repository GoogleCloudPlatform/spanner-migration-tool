import { Injectable } from '@angular/core'
import { FetchService } from '../fetch/fetch.service'
import IConv from '../../model/Conv'
import IRuleContent from 'src/app/model/Rule'
import { BehaviorSubject, forkJoin, Observable, of } from 'rxjs'
import { catchError, filter, map, tap } from 'rxjs/operators'
import IUpdateTable from 'src/app/model/updateTable'
import IDumpConfig from 'src/app/model/DumpConfig'
import ISessionConfig from '../../model/SessionConfig'
import { InputType, StorageKeys } from 'src/app/app.constants'
import ISession from 'src/app/model/Session'
import ISpannerConfig from '../../model/SpannerConfig'
import { SnackbarService } from '../snackbar/snackbar.service'

@Injectable({
  providedIn: 'root',
})
export class DataService {
  private convSubject = new BehaviorSubject<IConv>({} as IConv)
  private conversionRateSub = new BehaviorSubject({})
  private typeMapSub = new BehaviorSubject({})
  private summarySub = new BehaviorSubject({})
  private ddlSub = new BehaviorSubject({})
  private sessionsSub = new BehaviorSubject({} as ISession[])
  private configSub = new BehaviorSubject({} as ISpannerConfig)
  // currentSessionSub not using any where
  private currentSessionSub = new BehaviorSubject({} as ISession)
  private isOfflineSub = new BehaviorSubject<boolean>(false)
  private ruleMapSub = new BehaviorSubject<IRuleContent>({})

  rule = this.ruleMapSub.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))
  conv = this.convSubject.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))
  conversionRate = this.conversionRateSub
    .asObservable()
    .pipe(filter((res) => Object.keys(res).length !== 0))
  typeMap = this.typeMapSub.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))
  summary = this.summarySub.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))
  ddl = this.ddlSub.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))
  sessions = this.sessionsSub.asObservable()
  config = this.configSub.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))
  isOffline = this.isOfflineSub.asObservable()
  currentSession = this.currentSessionSub
    .asObservable()
    .pipe(filter((res) => Object.keys(res).length !== 0))

  constructor(private fetch: FetchService, private snackbar: SnackbarService) {
    let inputType = localStorage.getItem(StorageKeys.Type) as string
    let config: unknown = localStorage.getItem(StorageKeys.Config)

    switch (inputType) {
      case InputType.DirectConnect:
        this.getSchemaConversionFromDb()
        break

      case InputType.DumpFile:
        if (config !== null) {
          this.getSchemaConversionFromDump(config as IDumpConfig)
        }
        break

      case InputType.SessionFile:
        if (config !== null) {
          this.getSchemaConversionFromSession(config as ISessionConfig)
        }
        break

      default:
        console.log('not able to find input type')
    }
    this.getConfig()
    this.updateIsOffline()
  }

  resetStore() {
    this.convSubject.next({} as IConv)
    this.conversionRateSub.next({})
    this.typeMapSub.next({})
    this.summarySub.next({})
    this.ddlSub.next({})
  }

  getDdl() {
    this.fetch.getDdl().subscribe((res) => {
      this.ddlSub.next(res)
    })
  }

  getSchemaConversionFromDb() {
    this.fetch.getSchemaConversionFromDirectConnect().subscribe((res: IConv) => {
      this.convSubject.next(res)
      this.initiateSession()
    })
  }

  getAllSessions() {
    this.fetch.getSessions().subscribe({
      next: (sessions: ISession[]) => {
        this.sessionsSub.next(sessions)
      },
      error: (err: any) => {
        this.snackbar.openSnackBar('Not able to fetch Session..', 'Close', 5000)
      },
    })
  }

  getSchemaConversionFromDump(payload: IDumpConfig) {
    this.fetch.getSchemaConversionFromDump(payload).subscribe((res: IConv) => {
      this.convSubject.next(res)
      this.initiateSession()
    })
  }

  getSchemaConversionFromSession(payload: ISessionConfig) {
    this.fetch.getSchemaConversionFromSessionFile(payload).subscribe((res: IConv) => {
      this.convSubject.next(res)
      this.initiateSession()
    })
  }
  getSchemaConversionFromResumeSession(versionId: string) {
    this.fetch.resumeSession(versionId).subscribe((res: IConv) => {
      this.convSubject.next(res)
    })
  }

  getRateTypemapAndSummary() {
    return forkJoin({
      rates: this.fetch.getConversionRate(),
      typeMap: this.fetch.getTypeMap(),
      summary: this.fetch.getSummary(),
      ddl: this.fetch.getDdl(),
    })
      .pipe(
        catchError((err: any) => {
          console.log(err)
          return of(err)
        })
      )
      .subscribe(({ rates, typeMap, summary, ddl }: any) => {
        this.conversionRateSub.next(rates)
        this.typeMapSub.next(typeMap)
        this.summarySub.next(summary)
        this.ddlSub.next(ddl)
      })
  }

  updateTable(tableName: string, data: IUpdateTable): Observable<string> {
    return this.fetch.updateTable(tableName, data).pipe(
      catchError((e: any) => {
        return of({ error: e.error })
      }),
      tap(console.log),
      map((data: any) => {
        if (data.error) {
          return data.error
        } else {
          this.convSubject.next(data)
          return ''
        }
      })
    )
  }

  getConfig() {
    this.fetch.getSpannerConfig().subscribe((res: ISpannerConfig) => {
      this.configSub.next(res)
    })
  }

  updateConfig(config: ISpannerConfig) {
    this.configSub.next(config)
  }

  initiateSession() {
    this.fetch.InitiateSession().subscribe((data: any) => {
      console.log('get initiate session', data)
      this.currentSessionSub.next(data)
    })
  }

  updateGlobalType(types: Record<string, string>): void {
    this.fetch.updateGlobalType(types).subscribe({
      next: (data: any) => {
        this.convSubject.next(data)
        this.snackbar.openSnackBar('Type update successful.', 'Close', 3000)
      },
      error: (err: any) => {
        console.log(err)
        this.snackbar.openSnackBar('Unable to add rule', 'Close', 5000)
      },
    })
  }

  addRule(nextData: IRuleContent): void {
    this.ruleMapSub.next(nextData)
  }

  updateIsOffline() {
    this.fetch.getIsOffline().subscribe((res: boolean) => {
      this.isOfflineSub.next(res)
    })
  }
}
