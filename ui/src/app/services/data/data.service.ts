import { Injectable } from '@angular/core'
import { FetchService } from '../fetch/fetch.service'
import IConv from '../../model/Conv'
import { BehaviorSubject, forkJoin, Observable, of } from 'rxjs'
import { catchError, filter, map, tap } from 'rxjs/operators'
import IUpdateTable from 'src/app/model/updateTable'
import IDumpConfig from 'src/app/model/DumpConfig'
import ISessionConfig from '../../model/SessionConfig'

@Injectable({
  providedIn: 'root',
})
export class DataService {
  constructor(private fetch: FetchService) {}

  private convSubject = new BehaviorSubject<IConv>({} as IConv)
  private conversionRateSub = new BehaviorSubject({})
  private typeMapSub = new BehaviorSubject({})
  private summarySub = new BehaviorSubject({})
  private ddlSub = new BehaviorSubject({})

  conv = this.convSubject.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))
  conversionRate = this.conversionRateSub
    .asObservable()
    .pipe(filter((res) => Object.keys(res).length !== 0))
  typeMap = this.typeMapSub.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))
  summary = this.summarySub.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))
  ddl = this.ddlSub.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))

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

  getRateTypemapAndSummary() {
    forkJoin({
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
          console.log('data part --- ', data)
          this.convSubject.next(data)
          return ''
        }
      })
    )
  }
}
