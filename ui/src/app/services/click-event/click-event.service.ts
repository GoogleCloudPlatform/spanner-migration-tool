import { Injectable } from '@angular/core'
import { BehaviorSubject } from 'rxjs'

@Injectable({
  providedIn: 'root',
})
export class ClickEventService {
  private spannerConfigSub = new BehaviorSubject<boolean>(false)
  constructor() {}
  spannerConfig = this.spannerConfigSub.asObservable()

  openSpannerConfig() {
    this.spannerConfigSub.next(true)
  }
}
