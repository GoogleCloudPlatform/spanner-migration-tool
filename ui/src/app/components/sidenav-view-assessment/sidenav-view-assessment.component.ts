import { Component, OnInit } from '@angular/core'
import ConversionRate from 'src/app/model/conversion-rate'
import IViewAssesmentData from 'src/app/model/view-assesment'
import { ClickEventService } from 'src/app/services/click-event/click-event.service'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'
import IStructuredReport from '../../model/structured-report'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import * as JSZip from 'jszip'

@Component({
  selector: 'app-sidenav-view-assessment',
  templateUrl: './sidenav-view-assessment.component.html',
  styleUrls: ['./sidenav-view-assessment.component.scss'],
})
export class SidenavViewAssessmentComponent implements OnInit {
  structuredReport!: IStructuredReport
  srcDbType: string = ''
  connectionDetail: string = ''
  conversionRateCount: ConversionRate = { good: 0, ok: 0, bad: 0 }
  conversionRatePercentage: ConversionRate = { good: 0, ok: 0, bad: 0 }
  constructor(
    private sidenav: SidenavService, 
    private clickEvent: ClickEventService,
    private fetch: FetchService,
  ) {}
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

  // downloads structured report of the migration in JSON format
  downloadStructuredReport() {
    var a = document.createElement('a')
    this.fetch.getDStructuredReport().subscribe({
      next: (res: IStructuredReport) => {
        let resJson = JSON.stringify(res).replace(/9223372036854776000/g, '9223372036854775807')
        a.href = 'data:text;charset=utf-8,' + encodeURIComponent(resJson)
        let DB: string = res.summary.dbName
        a.download = `${DB}_migration_structuredReport.json`
        a.click()
      }
    })
  }

  //downloads text report of the migration in text format in more human readable form
  downloadTextReport(){
    var a = document.createElement('a')
    this.fetch.getDTextReport().subscribe({  
      next: (res: string) => {
        // calling this function to fetch 'database name' of the user database
        this.fetch.getDStructuredReport().subscribe({ 
          next: (prev: IStructuredReport) => {
            let DB: string = prev.summary.dbName
            a.href = 'data:text;charset=utf-8,' + encodeURIComponent(res)
            a.download = `${DB}_migration_textReport.txt`
            a.click()
          }
        })
      }
    })
  }

  downloadReports() {
    let zip = new JSZip()
    this.fetch.getDStructuredReport().subscribe({
      next: (resStructured: IStructuredReport) => {
        let fileNameHeader = resStructured.summary.dbName
        let resJson = JSON.stringify(resStructured).replace(/9223372036854776000/g, '9223372036854775807')
        let fileName = fileNameHeader + '_migration_structuredReport.json'
        // add the structured report in zip file
        zip.file(fileName, resJson)
        this.fetch.getDTextReport().subscribe({
          next: (resText: string) => {
            // add the text report in zip file
            zip.file(fileNameHeader + '_migration_textReport.txt', resText)
            // Generate the zip file asynchronously
            zip.generateAsync({ type: 'blob' })
              .then((blob: Blob) => {
                var a = document.createElement('a');
                a.href = URL.createObjectURL(blob);
                a.download = `${fileNameHeader}_reports`;
                a.click();
              })
          }
        })
      }
    })
  }
}