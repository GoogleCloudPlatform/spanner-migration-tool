import { Component, OnInit } from '@angular/core'
import ConversionRate from 'src/app/model/conversion-rate'
import IViewAssesmentData from 'src/app/model/view-assesment'
import { ClickEventService } from 'src/app/services/click-event/click-event.service'
import { SidenavService } from 'src/app/services/sidenav/sidenav.service'
import IStructuredReport, {IIssue, ITableReport} from '../../model/structured-report'
import { FetchService } from 'src/app/services/fetch/fetch.service'
import * as JSZip from 'jszip'
import IIssueReport, { TablesInformation } from 'src/app/model/issue-report'

export interface tableContent {
  position: number
  description: string
  tableCount: number
  tableNamesJoinedByComma: string
}

@Component({
  selector: 'app-sidenav-view-assessment',
  templateUrl: './sidenav-view-assessment.component.html',
  styleUrls: ['./sidenav-view-assessment.component.scss'],
})

export class SidenavViewAssessmentComponent implements OnInit {
  issueTableData!: tableContent
  structuredReport!: IStructuredReport
  issueTableData_Errors: tableContent[] = []
  issueTableData_Warnings: tableContent[] = []
  issueTableData_Suggestions: tableContent[] = []
  issueTableData_Notes: tableContent[] = []
  columnsToDisplay = ['position', 'description', 'tableCount']
  columnsToDisplayWithExpand = [...this.columnsToDisplay, 'expand'];
  expandedElements = new Set<any>();
  toggleRow(element: any) {
    if (this.isRowExpanded(element)) {
      this.expandedElements.delete(element);
    } else {
      this.expandedElements.add(element);
    }
  }
  isRowExpanded(element: any) {
    return this.expandedElements.has(element);
  }
  srcDbType: string = ''
  connectionDetail: string = ''
  summaryText: string = ''
  issueDescription: { [key: string]: string } = {}
  conversionRateCount: ConversionRate = { good: 0, ok: 0, bad: 0 }
  conversionRatePercentage: ConversionRate = { good: 0, ok: 0, bad: 0 }
  constructor(
    private sidenav: SidenavService,
    private clickEvent: ClickEventService,
    private fetch: FetchService,
  ) { }
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
      if (tableCount > 0) {
        this.setRateCountDataSource(tableCount)
      }
      this.fetch.getDStructuredReport().subscribe({
        next: (structuredReport) => {
          this.summaryText = structuredReport.summary.text
        }
      })
      this.issueTableData = {
        position: 0,
        description: '',
        tableCount: 0,
        tableNamesJoinedByComma: '',
      }
      this.fetch.getIssueDescription().subscribe({
        next: (issueDescription) => {
          this.issueDescription = issueDescription
          this.GenerateIssueReport()
        }
      })
    })
  }

  closeSidenav() {
    this.sidenav.closeSidenav()
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
  downloadTextReport() {
    var a = document.createElement('a')
    this.fetch.getDTextReport().subscribe({
      next: (res: string) => {
        let DB: string = this.connectionDetail
        a.href = 'data:text;charset=utf-8,' + encodeURIComponent(res)
        a.download = `${DB}_migration_textReport.txt`
        a.click()
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

  // manipulate the data fetched from structured report's tableReport to aggregate
  // tables by issue types and populate issueTableData_Errors, issueTableData_Warnings, 
  // issueTableData_Suggestions and issueTableData_Notes 
  // to work as dataSource required in the summarized table report
  GenerateIssueReport() {
    this.fetch.getDStructuredReport().subscribe({
      next: (resStructured: IStructuredReport) => {
        let fetchedTableReports = resStructured.tableReports
        var report: IIssueReport = {
          errors: new Map<string, TablesInformation>(),
          warnings: new Map<string, TablesInformation>(),
          suggestions: new Map<string, TablesInformation>(),
          notes: new Map<string, TablesInformation>(),
        }
        // iterate each table
        for (var fetchedTableReport of fetchedTableReports) {
          let allIssues = fetchedTableReport.issues

          // If this function is called again and if the number of issues in that session is zero, 
          // it will skip the for loop below. Therefore, we are setting the issueTableData to null here
          if (allIssues == null) {
            this.issueTableData_Errors = []
            this.issueTableData_Warnings = []
            this.issueTableData_Suggestions = []
            this.issueTableData_Notes = []
            return
          }

          // iterate each issue
          for (var issue of allIssues) {
            let defaultIssue: TablesInformation = {
              tableCount: 0,
              tableNames: new Set<string>(),
            }
            switch (issue.issueType) {
              case "Error":
              case "Errors":
                // store errors with table count and table names in report.errors 
                let errorIssues = issue.issueList
                this.AppendIssueWithTableInformation(errorIssues, report.errors, defaultIssue, fetchedTableReport)
                break

              case "Warnings":
              case "Warning":
                // store warnings with table count and table names in report.warnings
                let warningIssues = issue.issueList
                this.AppendIssueWithTableInformation(warningIssues, report.warnings, defaultIssue, fetchedTableReport)
                break

              case "Suggestion":
              case "Suggestions":
                // store suggestions with table count and table names in report.suggestions
                let suggestionIssues = issue.issueList
                this.AppendIssueWithTableInformation(suggestionIssues, report.suggestions, defaultIssue, fetchedTableReport)
                break

              case "Note":
              case "Notes":
                // store notes with table count and table names in report.notes
                let noteIssues = issue.issueList
                this.AppendIssueWithTableInformation(noteIssues, report.notes, defaultIssue, fetchedTableReport)
                break
            }
          }
        }

        // populate issueTableData_Warnings with data from report.warnings
        let map_report = report.warnings
        this.issueTableData_Warnings = []
        if (map_report.size != 0) {
          this.PopulateTableData(map_report, this.issueTableData_Warnings)
        }

        // populate issueTableData_Errors with data from report.errors
        map_report = report.errors
        this.issueTableData_Errors = []
        if (map_report.size != 0) {
          this.PopulateTableData(map_report, this.issueTableData_Errors)

        }

        // populate issueTableData_Suggestions with data from report.suggestions
        map_report = report.suggestions
        this.issueTableData_Suggestions = []
        if (map_report.size != 0) {
          this.PopulateTableData(map_report, this.issueTableData_Suggestions)
        }

        // populate issueTableData_Notes with data from report.notes
        map_report = report.notes
        this.issueTableData_Notes = []
        if (map_report.size != 0) {
          this.PopulateTableData(map_report, this.issueTableData_Notes)
        }
      }
    })
  }

  PopulateTableData(map_report: Map<string, TablesInformation>, issueTableData: tableContent[]) {
    let i = 1;
    for (let [key, value] of map_report.entries()) {
      let tableNamesList = [...value.tableNames.keys()]
      let fetchedDescription = this.issueDescription[key]
      issueTableData.push({
        position: i,
        description: fetchedDescription,
        tableCount: value.tableCount,
        tableNamesJoinedByComma: tableNamesList.join(', '),
      })
      i += 1;
    }
  }

  AppendIssueWithTableInformation(Issues: IIssue[], report: Map<string, TablesInformation>, defaultIssue: TablesInformation, fetchedTableReport: ITableReport) {
    for (var noteIssue of Issues) {
      let isPresent: boolean = report.has(noteIssue.category)
      
      // if the issue already exists in the report, we create a new issue description
      // and duplicate the existing one into it. This duplication is necessary because the value 
      // is passed by reference. After that, we add the table to that existing issue.
      if (isPresent) {
        let existingDesc = report.get(noteIssue.category)!;
        let descNew = {
          tableNames: new Set(existingDesc.tableNames),
          tableCount: existingDesc.tableNames.size
        }
        descNew.tableNames.add(fetchedTableReport.srcTableName)
        descNew.tableCount = descNew.tableNames.size
        report.set(noteIssue.category, descNew)
      } else {
        // if the issue is new we initialise issue description and add the table to it
        let desc = defaultIssue
        desc.tableNames.add(fetchedTableReport.srcTableName)
        desc.tableCount = desc.tableNames.size
        report.set(noteIssue.category, desc)
      }
    }
  }

}