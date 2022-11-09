import { Component, OnInit } from '@angular/core'
import ConversionRate from 'src/app/model/conversion-rate'
import IViewAssesmentData from 'src/app/model/view-assesment'
import { ClickEventService } from 'src/app/services/click-event/click-event.service'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'

@Component({
  selector: 'app-sidenav-view-assessment',
  templateUrl: './sidenav-view-assessment.component.html',
  styleUrls: ['./sidenav-view-assessment.component.scss'],
})
export class SidenavViewAssessmentComponent implements OnInit {
  srcDbType: string = ''
  connectionDetail: string = ''
  conversionRateCount: ConversionRate = { good: 0, ok: 0, bad: 0 }
  conversionRatePercentage: ConversionRate = { good: 0, ok: 0, bad: 0 }
  badPc = 50
  constructor(private sidenav: SidenavService, private clickEvent: ClickEventService) {}

  dbDataSource: { title: string; source: string; destination: string }[] = []
  dbDisplayedColumns: string[] = ['title', 'source', 'destination']
  rateCountDataSource: { total: number; bad: number; ok: number; good: number }[] = []
  rateCountDisplayedColumns: string[] = ['total', 'bad', 'ok', 'good']
  ratePcDataSource: { bad: number; ok: number; good: number }[] = []
  ratePcDisplayedColumns: string[] = ['bad', 'ok', 'good']

  ngOnInit(): void {
    this.clickEvent.viewAssesment.subscribe((data: IViewAssesmentData) => {
      this.srcDbType = data.srcDbType
      this.connectionDetail = data.connectionDetail
      this.conversionRateCount = data.conversionRates
      let tableCount: number =
        this.conversionRateCount.good + this.conversionRateCount.ok + this.conversionRateCount.bad
      if (tableCount > 0) {
        for (let key in this.conversionRatePercentage) {
          this.conversionRatePercentage[key as keyof ConversionRate] = Number(
            ((this.conversionRateCount[key as keyof ConversionRate] / tableCount) * 100).toFixed(2)
          )
        }
      }
      if (this.srcDbType != '') this.setDbDataSource()
      if (tableCount > 0) {
        this.setRateCountDataSource(tableCount)
      }
    })
  }

  closeSidenav() {
    this.sidenav.closeSidenav()
  }
  setDbDataSource() {
    this.dbDataSource = []
    this.dbDataSource.push({
      title: 'Database engine type',
      source: this.srcDbType,
      destination: 'Spanner',
    })
    this.dbDataSource.push({
      title: 'Connection details',
      source: this.connectionDetail,
      destination: 'Spanner',
    })
  }
  setRateCountDataSource(tableCount: number) {
    this.rateCountDataSource = []
    this.rateCountDataSource.push({
      total: tableCount,
      bad: this.conversionRateCount.bad,
      ok: this.conversionRateCount.ok,
      good: this.conversionRateCount.good,
    })
  }
}
