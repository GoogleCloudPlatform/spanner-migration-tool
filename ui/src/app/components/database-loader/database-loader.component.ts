import { Component, Input, OnInit } from '@angular/core'
import { Router } from '@angular/router'
import { ClickEventService } from 'src/app/services/click-event/click-event.service'

@Component({
  selector: 'app-database-loader',
  templateUrl: './database-loader.component.html',
  styleUrls: ['./database-loader.component.scss'],
})
export class DatabaseLoaderComponent implements OnInit {
  @Input() loaderType: string = ''
  @Input() databaseName: string = ''
  timeElapsed: number = 0
  timeElapsedInterval = setInterval(() => {
    this.timeElapsed += 1
  }, 1000)

  constructor(private router: Router, private clickEvent: ClickEventService) {}

  ngOnInit(): void {
    this.timeElapsed = 0
  }
  ngOnDestroy(): void {
    clearInterval(this.timeElapsedInterval)
  }
  cancelDbLoad() {
    this.clickEvent.cancelDbLoading()
    this.clickEvent.closeDatabaseLoader()
    this.router.navigate(['/'])
  }
}
