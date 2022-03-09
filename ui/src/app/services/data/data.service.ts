import { Injectable } from '@angular/core'
import { FetchService } from '../fetch/fetch.service'
import IConv from '../../model/Conv'
import { BehaviorSubject, forkJoin, Observable, of } from 'rxjs'
import { catchError, filter, map, tap } from 'rxjs/operators'
import IUpdateTable from 'src/app/model/updateTable'

@Injectable({
  providedIn: 'root',
})
export class DataService {
  private convSubject = new BehaviorSubject<IConv>({} as IConv)
  constructor(private fetch: FetchService) {}
  private conversionRateSub = new BehaviorSubject({})
  private typeMapSub = new BehaviorSubject({})
  private summarySub = new BehaviorSubject({})

  conv = this.convSubject.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))
  conversionRate = this.conversionRateSub
    .asObservable()
    .pipe(filter((res) => Object.keys(res).length !== 0))
  typeMap = this.typeMapSub.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))
  summary = this.summarySub.asObservable().pipe(filter((res) => Object.keys(res).length !== 0))

  getSchemaConversionData() {
    this.fetch.getSchemaConversionFromDirectConnect().subscribe((e: IConv) => {
      this.convSubject.next(e)
      forkJoin({
        rates: this.fetch.getConversionRate(),
        typeMap: this.fetch.getTypeMap(),
        summary: this.fetch.getSummary(),
      })
        .pipe(
          catchError((err: any) => {
            console.log(err)
            return of(err)
          })
        )
        .subscribe(({ rates, typeMap, summary }: any) => {
          this.conversionRateSub.next(rates)
          this.typeMapSub.next(typeMap)
          this.summarySub.next(summary)
        })
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
