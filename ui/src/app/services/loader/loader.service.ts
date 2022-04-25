import { Injectable } from '@angular/core'
import { BehaviorSubject } from 'rxjs'

@Injectable({
  providedIn: 'root',
})
export class LoaderService {
  private isLoadingSub = new BehaviorSubject<boolean>(false)
  constructor() {}
  isLoading = this.isLoadingSub.asObservable()

  startLoader() {
    this.isLoadingSub.next(true)
  }

  stopLoader() {
    this.isLoadingSub.next(false)
  }
}
