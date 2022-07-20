import { Injectable } from '@angular/core'
import { BehaviorSubject } from 'rxjs'

@Injectable({
  providedIn: 'root',
})
export class ClickEventService {
  private spannerConfigSub = new BehaviorSubject<boolean>(false)
  private datebaseLoaderSub = new BehaviorSubject<{ type: string; databaseName: string }>({
    type: '',
    databaseName: '',
  })
  constructor() {}
  spannerConfig = this.spannerConfigSub.asObservable()
  databaseLoader = this.datebaseLoaderSub.asObservable()

  openSpannerConfig() {
    this.spannerConfigSub.next(true)
  }
  openDatabaseLoader(type: string, databaseName: string) {
    this.datebaseLoaderSub.next({ type, databaseName })
  }
  closeDatabaseLoader() {
    this.datebaseLoaderSub.next({ type: '', databaseName: '' })
  }
}
