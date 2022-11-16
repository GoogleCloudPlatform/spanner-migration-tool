import { Injectable } from '@angular/core'
import { BehaviorSubject } from 'rxjs'
import IViewAssesmentData from 'src/app/model/view-assesment'

@Injectable({
  providedIn: 'root',
})
export class ClickEventService {
  private spannerConfigSub = new BehaviorSubject<boolean>(false)
  private datebaseLoaderSub = new BehaviorSubject<{ type: string; databaseName: string }>({
    type: '',
    databaseName: '',
  })
  private viewAssesmentSub = new BehaviorSubject<IViewAssesmentData>({
    srcDbType: '',
    connectionDetail: '',
    conversionRates: { good: 0, ok: 0, bad: 0 },
  })
  private tabToSpannerSub = new BehaviorSubject<boolean>(false)
  constructor() {}
  spannerConfig = this.spannerConfigSub.asObservable()
  databaseLoader = this.datebaseLoaderSub.asObservable()
  viewAssesment = this.viewAssesmentSub.asObservable()
  tabToSpanner = this.tabToSpannerSub.asObservable()
  openSpannerConfig() {
    this.spannerConfigSub.next(true)
  }
  openDatabaseLoader(type: string, databaseName: string) {
    this.datebaseLoaderSub.next({ type, databaseName })
  }
  closeDatabaseLoader() {
    this.datebaseLoaderSub.next({ type: '', databaseName: '' })
  }
  setViewAssesmentData(data: IViewAssesmentData) {
    this.viewAssesmentSub.next(data)
  }
  setTabToSpanner() {
    this.tabToSpannerSub.next(true)
  }
}
