import { Component, Input, OnInit } from '@angular/core'

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

  constructor() {}

  ngOnInit(): void {
    this.timeElapsed = 0
  }
  ngOnDestroy(): void {
    clearInterval(this.timeElapsedInterval)
  }
}
