import { Injectable } from '@angular/core'
import { FetchService } from '../fetch/fetch.service'
import IConv from '../../model/Conv'
import { BehaviorSubject, forkJoin, Observable, of } from 'rxjs'
import { catchError, filter, map, tap } from 'rxjs/operators'
import IUpdateTable from 'src/app/model/updateTable'
import IDumpConfig from 'src/app/model/DumpConfig'
import ISessionConfig from '../../model/SessionConfig'
import { InputType, StorageKeys } from 'src/app/app.constants'
import { LoaderService } from 'src/app/services/loader/loader.service'
import ISession from 'src/app/model/Session'

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

  conv = this.convSubject.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))
  conversionRate = this.conversionRateSub
    .asObservable()
    .pipe(filter((res) => Object.keys(res).length !== 0))
  typeMap = this.typeMapSub.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))
  summary = this.summarySub.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))
  ddl = this.ddlSub.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))
  sessions = this.sessionsSub.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))

  constructor(private fetch: FetchService, private loader: LoaderService) {
    let inputType = localStorage.getItem(StorageKeys.Type) as string
    let config: unknown = localStorage.getItem(StorageKeys.Config)
    console.log(inputType, config)

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

    this.fetch.getSessions().subscribe((sessions: ISession[]) => {
      this.sessionsSub.next(sessions)
    })
  }

  resetStore() {
    this.convSubject.next({} as IConv)
    this.conversionRateSub.next({})
    this.typeMapSub.next({})
    this.summarySub.next({})
    this.ddlSub.next({})
  }

  getSchemaConversionFromDb() {
    this.fetch.getSchemaConversionFromDirectConnect().subscribe((res: IConv) => {
      this.convSubject.next(res)
    })
  }

  getSchemaConversionFromDump(payload: IDumpConfig) {
    this.fetch.getSchemaConversionFromDump(payload).subscribe((res: IConv) => {
      this.convSubject.next(res)
    })
  }

  getSchemaConversionFromSession(payload: ISessionConfig) {
    this.fetch.getSchemaConversionFromSessionFile(payload).subscribe((res: IConv) => {
      this.convSubject.next(res)
    })
  }
  getSchemaConversionFromResumeSession(versionId: string) {
    this.fetch.resumeSession(versionId).subscribe((res: IConv) => {
      console.log(res)
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
        console.log('new data from.... conv', rates, typeMap, summary, ddl)

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
          console.log('data part --- ', data)
          this.convSubject.next(data)
          return ''
        }
      })
    )
  }
}
