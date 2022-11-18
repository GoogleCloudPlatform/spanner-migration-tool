import { Injectable } from '@angular/core'
import { FetchService } from '../fetch/fetch.service'
import IConv, { ICreateIndex, IInterleaveStatus, IPrimaryKey } from '../../model/conv'
import IRuleContent, { IRule } from 'src/app/model/rule'
import { BehaviorSubject, forkJoin, Observable, of } from 'rxjs'
import { catchError, filter, map, tap } from 'rxjs/operators'
import IUpdateTable from 'src/app/model/update-table'
import IDumpConfig from 'src/app/model/dump-config'
import ISessionConfig from '../../model/session-config'
import ISession from 'src/app/model/session'
import ISpannerConfig from '../../model/spanner-config'
import { SnackbarService } from '../snackbar/snackbar.service'
import ISummary from 'src/app/model/summary'
import { ClickEventService } from '../click-event/click-event.service'
import { TableUpdatePubSubService } from '../table-update-pub-sub/table-update-pub-sub.service'

@Injectable({
  providedIn: 'root',
})
export class DataService {
  private convSubject = new BehaviorSubject<IConv>({} as IConv)
  private conversionRateSub = new BehaviorSubject({})
  private typeMapSub = new BehaviorSubject({})
  private summarySub = new BehaviorSubject(new Map<string, ISummary>())
  private ddlSub = new BehaviorSubject({})
  private tableInterleaveStatusSub = new BehaviorSubject({} as IInterleaveStatus)
  private sessionsSub = new BehaviorSubject({} as ISession[])
  private configSub = new BehaviorSubject({} as ISpannerConfig)
  // currentSessionSub not using any where
  private currentSessionSub = new BehaviorSubject({} as ISession)
  private isOfflineSub = new BehaviorSubject<boolean>(false)
  private ruleMapSub = new BehaviorSubject<IRule>({})

  rule = this.ruleMapSub.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))
  conv = this.convSubject.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))
  conversionRate = this.conversionRateSub
    .asObservable()
    .pipe(filter((res) => Object.keys(res).length !== 0))
  typeMap = this.typeMapSub.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))
  summary = this.summarySub.asObservable().pipe(filter((res) => res.size >= 0))
  ddl = this.ddlSub.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))
  tableInterleaveStatus = this.tableInterleaveStatusSub.asObservable()
  sessions = this.sessionsSub.asObservable()
  config = this.configSub.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))
  isOffline = this.isOfflineSub.asObservable()
  currentSession = this.currentSessionSub
    .asObservable()
    .pipe(filter((res) => Object.keys(res).length !== 0))

  constructor(
    private fetch: FetchService,
    private snackbar: SnackbarService,
    private clickEvent: ClickEventService,
    private tableUpdatePubSub: TableUpdatePubSubService
  ) {
    this.getLastSessionDetails()
    this.getConfig()
    this.updateIsOffline()
  }

  resetStore() {
    this.convSubject.next({} as IConv)
    this.conversionRateSub.next({})
    this.typeMapSub.next({})
    this.summarySub.next(new Map<string, ISummary>())
    this.ddlSub.next({})
    this.tableInterleaveStatusSub.next({} as IInterleaveStatus)
  }

  getDdl() {
    this.fetch.getDdl().subscribe((res) => {
      this.ddlSub.next(res)
    })
  }

  getSchemaConversionFromDb() {
    this.fetch.getSchemaConversionFromDirectConnect().subscribe((res: IConv) => {
      this.convSubject.next(res)
    })
  }

  //To do : remove snackbar call from dataservice
  getAllSessions() {
    this.fetch.getSessions().subscribe({
      next: (sessions: ISession[]) => {
        this.sessionsSub.next(sessions)
      },
      error: (err: any) => {
        this.snackbar.openSnackBar('Unable to fetch sessions.', 'Close')
      },
    })
  }

  getLastSessionDetails() {
    this.fetch.getLastSessionDetails().subscribe({
      next: (res: IConv) => {
        this.convSubject.next(res)
      },
      error: (err: any) => {
        this.snackbar.openSnackBar(err.error, 'Close')
      },
    })
  }

  getSchemaConversionFromDump(payload: IDumpConfig) {
    this.fetch.getSchemaConversionFromDump(payload).subscribe({
      next: (res: IConv) => {
        this.convSubject.next(res)
      },
      error: (err: any) => {
        this.clickEvent.closeDatabaseLoader()
        this.snackbar.openSnackBar(err.error, 'Close')
      },
    })
  }

  getSchemaConversionFromSession(payload: ISessionConfig) {
    this.fetch.getSchemaConversionFromSessionFile(payload).subscribe({
      next: (res: IConv) => {
        this.convSubject.next(res)
      },
      error: (err: any) => {
        this.snackbar.openSnackBar(err.error, 'Close')
        this.clickEvent.closeDatabaseLoader()
      },
    })
  }

  getSchemaConversionFromResumeSession(versionId: string) {
    this.fetch.resumeSession(versionId).subscribe({
      next: (res: IConv) => {
        this.convSubject.next(res)
      },
      error: (err: any) => {
        this.snackbar.openSnackBar(err.error, 'Close')
      },
    })
  }

  getConversionRate() {
    this.fetch.getConversionRate().subscribe((res) => {
      this.conversionRateSub.next(res)
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
          return of(err)
        })
      )
      .subscribe(({ rates, typeMap, summary, ddl }: any) => {
        this.conversionRateSub.next(rates)
        this.typeMapSub.next(typeMap)
        this.summarySub.next(new Map<string, ISummary>(Object.entries(summary)))
        this.ddlSub.next(ddl)
      })
  }
  getSummary() {
    return this.fetch.getSummary().subscribe({
      next: (summary: any) => {
        this.summarySub.next(new Map<string, ISummary>(Object.entries(summary)))
      },
    })
  }

  reviewTableUpdate(tableName: string, data: IUpdateTable): Observable<string> {
    return this.fetch.reviewTableUpdate(tableName, data).pipe(
      catchError((e: any) => {
        return of({ error: e.error })
      }),
      tap(console.log),
      map((data: any) => {
        if (data.error) {
          return data.error
        } else {
          this.tableUpdatePubSub.setTableReviewChanges(data)
          return ''
        }
      })
    )
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
          this.getDdl()
          return ''
        }
      })
    )
  }

  removeInterleave(tableId: string): Observable<string> {
    return this.fetch.removeInterleave(tableId).pipe(
      catchError((e: any) => {
        return of({ error: e.error })
      }),
      tap(console.log),
      map((data) => {
        this.getDdl()
        if (data.error) {
          this.snackbar.openSnackBar(data.error, 'Close')
          return data.error
        } else {
          this.convSubject.next(data)
          return ''
        }
      })
    )
  }

  restoreTable(tableId: string): Observable<string> {
    return this.fetch.restoreTable(tableId).pipe(
      catchError((e: any) => {
        return of({ error: e.error })
      }),
      tap(console.log),
      map((data) => {
        if (data.error) {
          this.snackbar.openSnackBar(data.error, 'Close')
          return data.error
        } else {
          this.convSubject.next(data)
          this.snackbar.openSnackBar('Table restored successfully', 'Close', 5)
          return ''
        }
      })
    )
  }

  dropTable(tableId: string): Observable<string> {
    return this.fetch.dropTable(tableId).pipe(
      catchError((e: any) => {
        return of({ error: e.error })
      }),
      tap(console.log),
      map((data) => {
        if (data.error) {
          this.snackbar.openSnackBar(data.error, 'Close')
          return data.error
        } else {
          this.convSubject.next(data)
          this.snackbar.openSnackBar('Table dropped successfully', 'Close', 5)
          return ''
        }
      })
    )
  }

  updatePk(pkObj: IPrimaryKey) {
    return this.fetch.updatePk(pkObj).pipe(
      catchError((e: any) => {
        return of({ error: e.error })
      }),
      tap(console.log),
      map((data: any) => {
        if (data.error) {
          return data.error
        } else {
          this.convSubject.next(data)
          this.getDdl()
          return ''
        }
      })
    )
  }

  updateFkNames(tableName: string, updatedFkNames: Record<string, string>): Observable<string> {
    return this.fetch.updateFk(tableName, updatedFkNames).pipe(
      catchError((e: any) => {
        return of({ error: e.error })
      }),
      tap(console.log),
      map((data: any) => {
        if (data.error) {
          return data.error
        } else {
          this.convSubject.next(data)
          this.getDdl()
          return ''
        }
      })
    )
  }

  dropFk(tableName: string, fkName: string) {
    return this.fetch.removeFk(tableName, fkName).pipe(
      catchError((e: any) => {
        return of({ error: e.error })
      }),
      tap(console.log),
      map((data: any) => {
        if (data.error) {
          return data.error
        } else {
          this.convSubject.next(data)
          this.getDdl()
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
      this.currentSessionSub.next(data)
    })
  }

  updateGlobalType(types: Record<string, string>): void {
    this.fetch.updateGlobalType(types).subscribe({
      next: (data: any) => {
        this.convSubject.next(data)
        this.snackbar.openSnackBar('Global datatype updated successfully', 'Close', 5)
        this.getSummary()
        this.getDdl()
      },
      error: (err: any) => {
        this.snackbar.openSnackBar('Unable to add rule', 'Close')
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

  addIndex(tableName: string, payload: ICreateIndex[]) {
    this.fetch.addIndex(tableName, payload).subscribe({
      next: (res: IConv) => {
        this.convSubject.next(res)
        this.getDdl()
        this.snackbar.openSnackBar('Added new index.', 'Close', 5)
      },
      error: (err: any) => {
        this.snackbar.openSnackBar(err.error, 'Close')
      },
    })
  }

  applyRule(payload: IRule) {
    this.fetch.applyRule(payload).subscribe({
      next: (res: any) => {
        this.ruleMapSub.next(res?.Rules)
        this.snackbar.openSnackBar('Added new rule.', 'Close', 5)
      },
      error: (err: any) => {
        this.snackbar.openSnackBar(err.error, 'Close')
      },
    })
  }

  updateIndex(tableName: string, payload: ICreateIndex[]) {
    return this.fetch.updateIndex(tableName, payload).pipe(
      catchError((e: any) => {
        return of({ error: e.error })
      }),
      tap(console.log),
      map((data: any) => {
        if (data.error) {
          return data.error
        } else {
          this.convSubject.next(data)
          this.getDdl()
          return ''
        }
      })
    )
  }

  dropIndex(tableName: string, indexName: string): Observable<string> {
    return this.fetch.dropIndex(tableName, indexName).pipe(
      catchError((e: any) => {
        return of({ error: e.error })
      }),
      tap(console.log),
      map((data) => {
        if (data.error) {
          this.snackbar.openSnackBar(data.error, 'Close')
          return data.error
        } else {
          this.convSubject.next(data)
          this.getDdl()
          this.snackbar.openSnackBar('Index dropped successfully', 'Close', 5)
          return ''
        }
      })
    )
  }

  restoreIndex(tableId: string, indexId: string): Observable<string> {
    return this.fetch.restoreIndex(tableId, indexId).pipe(
      catchError((e: any) => {
        return of({ error: e.error })
      }),
      tap(console.log),
      map((data) => {
        if (data.error) {
          this.snackbar.openSnackBar(data.error, 'Close')
          return data.error
        } else {
          this.convSubject.next(data)
          this.snackbar.openSnackBar('Index restored successfully', 'Close', 5)
          return ''
        }
      })
    )
  }

  getInterleaveConversionForATable(tableName: string) {
    this.fetch.getInterleaveStatus(tableName).subscribe((res: IInterleaveStatus) => {
      this.tableInterleaveStatusSub.next(res)
    })
  }

  setInterleave(tableName: string) {
    this.fetch.setInterleave(tableName).subscribe((res: any) => {
      this.getDdl()
      if (res.sessionState) {
        this.convSubject.next(res.sessionState as IConv)
      }
    })
  }
}
