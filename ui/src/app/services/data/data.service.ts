import { Injectable } from '@angular/core'
import { FetchService } from '../fetch/fetch.service'
import IConv, { ICreateIndex, IForeignKey, IInterleaveStatus, IPrimaryKey } from '../../model/conv'
import IRule from 'src/app/model/rule'
import { BehaviorSubject, forkJoin, Observable, of } from 'rxjs'
import { catchError, filter, map, tap } from 'rxjs/operators'
import IUpdateTable, { IAddColumn, IReviewInterleaveTableChanges, ITableColumnChanges } from 'src/app/model/update-table'
import IDumpConfig, { IConvertFromDumpRequest } from 'src/app/model/dump-config'
import ISessionConfig from '../../model/session-config'
import ISession from 'src/app/model/session'
import ISpannerConfig from '../../model/spanner-config'
import { SnackbarService } from '../snackbar/snackbar.service'
import ISummary from 'src/app/model/summary'
import { ClickEventService } from '../click-event/click-event.service'
import { TableUpdatePubSubService } from '../table-update-pub-sub/table-update-pub-sub.service'
import { ConversionService } from '../conversion/conversion.service'
import { ColLength, Dialect } from 'src/app/app.constants'
import { ITables } from 'src/app/model/migrate'

@Injectable({
  providedIn: 'root',
})
export class DataService {
  private convSubject = new BehaviorSubject<IConv>({} as IConv)
  private conversionRateSub = new BehaviorSubject({})
  private typeMapSub = new BehaviorSubject({})
  private defaultTypeMapSub = new BehaviorSubject({})
  private summarySub = new BehaviorSubject(new Map<string, ISummary>())
  private ddlSub = new BehaviorSubject({})
  private tableInterleaveStatusSub = new BehaviorSubject({} as IInterleaveStatus)
  private sessionsSub = new BehaviorSubject({} as ISession[])
  private configSub = new BehaviorSubject({} as ISpannerConfig)
  // currentSessionSub not using any where
  private currentSessionSub = new BehaviorSubject({} as ISession)
  private isOfflineSub = new BehaviorSubject<boolean>(false)
  private ruleMapSub = new BehaviorSubject<IRule[]>([])

  rule = this.ruleMapSub.asObservable()
  conv = this.convSubject.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))
  conversionRate = this.conversionRateSub
    .asObservable()
    .pipe(filter((res) => Object.keys(res).length !== 0))
  typeMap = this.typeMapSub.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))
  defaultTypeMap = this.defaultTypeMapSub.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))
  summary = this.summarySub.asObservable()
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
    private tableUpdatePubSub: TableUpdatePubSubService,
    private conversion: ConversionService
  ) {
    this.getLastSessionDetails()
    this.getConfig()
    this.updateIsOffline()
  }

  resetStore() {
    this.convSubject.next({} as IConv)
    this.conversionRateSub.next({})
    this.typeMapSub.next({})
    this.defaultTypeMapSub.next({})
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
    this.fetch.getSchemaConversionFromDirectConnect().subscribe({
      next: (res: IConv) => {
        this.convSubject.next(res)
        this.ruleMapSub.next(res?.Rules)
      },
      error: (err: any) => {
        this.clickEvent.closeDatabaseLoader()
        this.snackbar.openSnackBar(err.error, 'Close')
      },
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
        this.ruleMapSub.next(res?.Rules)
      },
      error: (err: any) => {
        this.snackbar.openSnackBar(err.error, 'Close')
      },
    })
  }

  getSchemaConversionFromDump(payload: IConvertFromDumpRequest) {
    return this.fetch.getSchemaConversionFromDump(payload).subscribe({
      next: (res: IConv) => {
        this.convSubject.next(res)
        this.ruleMapSub.next(res?.Rules)
      },
      error: (err: any) => {
        this.clickEvent.closeDatabaseLoader()
        this.snackbar.openSnackBar(err.error, 'Close')
      },
    })
  }

  getSchemaConversionFromSession(payload: ISessionConfig) {
    return this.fetch.getSchemaConversionFromSessionFile(payload).subscribe({
      next: (res: IConv) => {
        this.convSubject.next(res)
        this.ruleMapSub.next(res?.Rules)
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
        this.ruleMapSub.next(res?.Rules)
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
      defaultTypeMap: this.fetch.getSpannerDefaultTypeMap(),
      summary: this.fetch.getSummary(),
      ddl: this.fetch.getDdl(),
    })
      .pipe(
        catchError((err: any) => {
          return of(err)
        })
      )
      .subscribe(({ rates, typeMap,defaultTypeMap, summary, ddl }: any) => {
        this.conversionRateSub.next(rates)
        this.typeMapSub.next(typeMap)
        this.defaultTypeMapSub.next(defaultTypeMap)
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

  reviewTableUpdate(tableId: string, data: IUpdateTable): Observable<string> {
    return this.fetch.reviewTableUpdate(tableId, data).pipe(
      catchError((e: any) => {
        return of({ error: e.error })
      }),
      tap(console.log),
      map((data: any) => {
        if (data.error) {
          return data.error
        } else {
          let standardDatatypeToPGSQLTypemap: Map<String, String>;
          this.conversion.standardTypeToPGSQLTypeMap.subscribe((typemap) => {
            standardDatatypeToPGSQLTypemap = typemap
          })
          this.conv.subscribe((convData: IConv) => {

            data.Changes.forEach((table: IReviewInterleaveTableChanges) => {
              table.InterleaveColumnChanges.forEach((column: ITableColumnChanges) => {
                if (convData.SpDialect === Dialect.PostgreSQLDialect) {
                  let pgSQLType = standardDatatypeToPGSQLTypemap.get(column.Type)
                  let pgSQLUpdateType = standardDatatypeToPGSQLTypemap.get(column.UpdateType)
                  column.Type = pgSQLType === undefined ? column.Type : pgSQLType
                  column.UpdateType = pgSQLUpdateType === undefined ? column.UpdateType : pgSQLUpdateType
                }
                if (ColLength.DataTypes.indexOf(column.Type.toString())>-1) {
                  column.Type += this.updateColumnSize(column.Size)
                }
                if (ColLength.DataTypes.indexOf(column.UpdateType.toString())>-1) {
                  column.UpdateType += this.updateColumnSize(column.UpdateSize)
                }
              })
            })
          })
          this.tableUpdatePubSub.setTableReviewChanges(data)
          return ''
        }
      })
    )
  }

  updateColumnSize(size: Number): string {
    if (size === ColLength.StorageMaxLength) {
      return '(MAX)'
    } else {
      return '(' + size + ')'
    }
  }

  updateTable(tableId: string, data: IUpdateTable): Observable<string> {
    return this.fetch.updateTable(tableId, data).pipe(
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

  restoreTables(tables: ITables): Observable<string> {
    return this.fetch.restoreTables(tables).pipe(
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
          this.snackbar.openSnackBar('Selected tables restored successfully', 'Close', 5)
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
          this.snackbar.openSnackBar('Table skipped successfully', 'Close', 5)
          return ''
        }
      })
    )
  }

  dropTables(tables: ITables): Observable<string> {
    return this.fetch.dropTables(tables).pipe(
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
          this.snackbar.openSnackBar('Selected tables skipped successfully', 'Close', 5)
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

  updateFkNames(tableId: string, updatedFk: IForeignKey[]): Observable<string> {
    return this.fetch.updateFk(tableId, updatedFk).pipe(
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

  dropFk(tableId: string, fkId: string) {
    return this.fetch.removeFk(tableId, fkId).pipe(
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

  updateIsOffline() {
    this.fetch.getIsOffline().subscribe((res: boolean) => {
      this.isOfflineSub.next(res)
    })
  }

  addColumn(tableId: string,payload: IAddColumn) {
    this.fetch.addColumn(tableId,payload).subscribe({
      next: (res: any) => {
        this.convSubject.next(res)
        this.getDdl()
        this.snackbar.openSnackBar('Added new column.', 'Close', 5)
      },
      error: (err: any) => {
        this.snackbar.openSnackBar(err.error, 'Close')
      },
    })
  }

  applyRule(payload: IRule) {
    this.fetch.applyRule(payload).subscribe({
      next: (res: any) => {
        this.convSubject.next(res)
        this.ruleMapSub.next(res?.Rules)
        this.getDdl()
        this.snackbar.openSnackBar('Added new rule.', 'Close', 5)
      },
      error: (err: any) => {
        this.snackbar.openSnackBar(err.error, 'Close')
      },
    })
  }
  updateIndex(tableId: string, payload: ICreateIndex[]) {
    return this.fetch.updateIndex(tableId, payload).pipe(
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

  dropIndex(tableId: string, indexId: string): Observable<string> {
    return this.fetch.dropIndex(tableId, indexId).pipe(
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
          this.ruleMapSub.next(data?.Rules)
          this.snackbar.openSnackBar('Index skipped successfully', 'Close', 5)
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

  getInterleaveConversionForATable(tableId: string) {
    this.fetch.getInterleaveStatus(tableId).subscribe((res: IInterleaveStatus) => {
      this.tableInterleaveStatusSub.next(res)
    })
  }

  setInterleave(tableId: string) {
    this.fetch.setInterleave(tableId).subscribe((res: any) => {
      this.convSubject.next(res.sessionState)
      this.getDdl()
      if (res.sessionState) {
        this.convSubject.next(res.sessionState as IConv)
      }
    })
  }

  uploadFile(file: FormData): Observable<string> {
    return this.fetch.uploadFile(file).pipe(
      catchError((e: any) => {
        return of({ error: e.error })
      }),
      tap(console.log),
      map((data) => {
        if (data.error) {
          this.snackbar.openSnackBar('File upload failed', 'Close')
          return data.error
        } else {
          this.snackbar.openSnackBar('File uploaded successfully', 'Close', 5)
          return ''
        }
      })
    )
  }

  dropRule(ruleId: string) {
    return this.fetch.dropRule(ruleId).subscribe({
      next: (res: any) => {
        this.convSubject.next(res)
        this.ruleMapSub.next(res?.Rules)
        this.getDdl()
        this.snackbar.openSnackBar('Rule deleted successfully', 'Close', 5)
      },
      error: (err: any) => {
        this.snackbar.openSnackBar(err.error, 'Close')
      },
    })
  }

  verifyJsonCfg(configuration : string): Observable<string> {
    return this.fetch.verifyJsonConfiguration(configuration).pipe(
      catchError((e: any) => {
        return of({ error: e.error })
      }),
      tap(console.log),
      map((data: any) => {
        if (data.error) {
          return data.error
        } else {
          return ''
        }
      })
    )
  }
}
