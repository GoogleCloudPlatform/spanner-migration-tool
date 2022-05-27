import { Component, OnInit } from '@angular/core'
import { ClickEventService } from 'src/app/services/click-event/click-event.service'

@Component({
  selector: 'app-source-selection',
  templateUrl: './source-selection.component.html',
  styleUrls: ['./source-selection.component.scss'],
})
export class SourceSelectionComponent implements OnInit {
  isDatabaseLoading: boolean = false
  loaderType: string = ''
  databaseName: string = ''
  constructor(private clickevent: ClickEventService) {}

  ngOnInit(): void {
    this.clickevent.databaseLoader.subscribe((response) => {
      this.loaderType = response.type
      this.databaseName = response.databaseName
      if (this.loaderType !== '') {
        this.isDatabaseLoading = true
      } else {
        this.isDatabaseLoading = false
      }
    })
  }
}
