import { Injectable } from '@angular/core'
import { FetchService } from '../fetch/fetch.service'
import IConv from '../../model/Conv'
import { BehaviorSubject } from 'rxjs'
import { filter, tap } from 'rxjs/operators'

@Injectable({
  providedIn: 'root',
})
export class DataService {
  private convSubject = new BehaviorSubject<IConv>({} as IConv)
  constructor(private fetch: FetchService) {}

  conv = this.convSubject.asObservable().pipe(
    tap(console.log),
    filter((res) => Object.keys(res).length !== 0),
    tap(console.log)
  )

  getSchemaConversionData() {
    this.fetch.getSchemaConversion().subscribe((res: IConv) => {
      console.log(res)
      this.convSubject.next(res)
    })
  }
}
