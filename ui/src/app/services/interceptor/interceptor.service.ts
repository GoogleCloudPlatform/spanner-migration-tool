import { HttpEvent, HttpHandler, HttpInterceptor, HttpRequest } from '@angular/common/http'
import { Injectable } from '@angular/core'
import { finalize, Observable } from 'rxjs'
import { LoaderService } from '../loader/loader.service'

@Injectable({
  providedIn: 'root',
})
export class InterceptorService implements HttpInterceptor {
  count: number = 0
  constructor(private loader: LoaderService) {}
  intercept(req: HttpRequest<any>, next: HttpHandler): Observable<HttpEvent<any>> {
    if (req.url.includes('/connect')) {
      this.loader.startLoader()
      this.count++
    }
    return next.handle(req).pipe(
      finalize(() => {
        this.count--
        if (this.count == 0) {
          this.loader.stopLoader()
        }
      })
    )
  }
}
