import { Component, Input, OnInit, Output, SimpleChanges, EventEmitter } from '@angular/core'
import { DataService } from 'src/app/services/data/data.service'
import ISummary, { ISummaryRow } from 'src/app/model/summary'
import { FlatNode } from 'src/app/model/schema-object-node'
import { Observable, of } from 'rxjs'
import { map, startWith } from 'rxjs/operators'
import { FormControl } from '@angular/forms'
import { ClickEventService } from 'src/app/services/click-event/click-event.service'

@Component({
  selector: 'app-summary',
  templateUrl: './summary.component.html',
  styleUrls: ['./summary.component.scss'],
})
export class SummaryComponent implements OnInit {
  @Output() changeIssuesLabel: EventEmitter<number> = new EventEmitter<number>()
  summaryRows: ISummaryRow[] = []
  summary: Map<string, ISummary> = new Map<string, ISummary>()
  filteredSummaryRows: ISummaryRow[] = []
  readonly separatorKeysCodes = [] as const
  summaryCount: number = 0
  totalNoteCount: number = 0
  totalWarningCount: number = 0
  totalSuggestionCount: number = 0
  totalErrorCount: number = 0

  filterInput = new FormControl()
  options: string[] = ['read', 'unread', 'warning', 'suggestion', 'note', 'error']
  obsFilteredOptions: Observable<string[]> = new Observable<string[]>()
  searchFilters: string[] = ['unread', 'warning', 'note', 'suggestion', 'error']

  @Input() currentObject: FlatNode | null = null
  constructor(private data: DataService, private clickEvent: ClickEventService) { }

  ngOnInit(): void {
    this.data.summary.subscribe({
      next: (summary: Map<string, ISummary>) => {
        this.summary = summary
        if (this.currentObject) {
          let objectId = this.currentObject.id
          if (this.currentObject.type == 'indexName') {
            objectId = this.currentObject.parentId
          }
          let s = this.summary.get(objectId)
          if (s) {
            this.summaryRows = []
            this.initiateSummaryCollection(s)
            this.applyFilters()
            this.summaryCount = s.NotesCount + s.WarningsCount + s.ErrorsCount + s.SuggestionsCount
            this.changeIssuesLabel.emit(
              s.NotesCount + s.WarningsCount + s.ErrorsCount + s.SuggestionsCount
            )
          } else {
            this.summaryCount = 0
            this.changeIssuesLabel.emit(0)
          }
        } else {
          this.initialiseSummaryCollectionForAllTables(this.summary)
          this.summaryCount = this.totalNoteCount + this.totalErrorCount + this.totalSuggestionCount + this.totalWarningCount
          this.changeIssuesLabel.emit(
            this.summaryCount
          )
        }
      },
    })

    this.registerAutoCompleteChange()
  }

  initialiseSummaryCollectionForAllTables(summaryMap: Map<string, ISummary>) {
    this.summaryRows = []
    this.totalErrorCount = 0
    this.totalNoteCount = 0
    this.totalSuggestionCount = 0
    this.totalWarningCount = 0
    for (const summary of summaryMap.values()) {
      this.initiateSummaryCollection(summary)
    }
    this.applyFilters()
  }

  ngOnChanges(changes: SimpleChanges): void {
    this.currentObject = changes?.['currentObject']?.currentValue || this.currentObject
    this.summaryRows = []
    if (this.currentObject) {
      let objectId = this.currentObject.id
      if (this.currentObject.type == 'indexName') {
        objectId = this.currentObject.parentId
      }
      let s = this.summary.get(objectId)
      if (s) {
        this.summaryRows = []
        this.initiateSummaryCollection(s)
        this.applyFilters()
        this.summaryCount = s.NotesCount + s.WarningsCount + s.ErrorsCount + s.SuggestionsCount
        this.changeIssuesLabel.emit(
          s.NotesCount + s.WarningsCount + s.ErrorsCount + s.SuggestionsCount
        )
      } else {
        this.summaryCount = 0
        this.changeIssuesLabel.emit(0)
      }
    } else {
      this.summaryCount = 0
      this.changeIssuesLabel.emit(0)
    }
  }

  initiateSummaryCollection(summary: ISummary) {

    this.totalErrorCount += summary.ErrorsCount
    this.totalNoteCount += summary.NotesCount
    this.totalWarningCount += summary.WarningsCount
    this.totalSuggestionCount += summary.SuggestionsCount

    summary.Errors.forEach((v) => {
      this.summaryRows.push({
        type: 'error',
        content: v,
        isRead: false,
      })
    })
    summary.Warnings.forEach((v) => {
      this.summaryRows.push({
        type: 'warning',
        content: v,
        isRead: false,
      })
    })
    summary.Suggestions.forEach((v) => {
      this.summaryRows.push({
        type: 'suggestion',
        content: v,
        isRead: false,
      })
    })
    summary.Notes.forEach((v) => {
      this.summaryRows.push({
        type: 'note',
        content: v,
        isRead: false,
      })
    })
  }

  applyFilters() {
    let typeFilters: Array<(data: ISummaryRow) => Boolean> = []
    let readFilters: Array<(data: ISummaryRow) => Boolean> = []

    if (this.searchFilters.includes('read')) {
      readFilters.push((s: ISummaryRow) => s.isRead)
    }
    if (this.searchFilters.includes('unread')) {
      readFilters.push((s: ISummaryRow) => !s.isRead)
    }

    if (this.searchFilters.includes('warning')) {
      typeFilters.push((s: ISummaryRow) => s.type == 'warning')
    }
    if (this.searchFilters.includes('note')) {
      typeFilters.push((s: ISummaryRow) => s.type == 'note')
    }
    if (this.searchFilters.includes('suggestion')) {
      typeFilters.push((s: ISummaryRow) => s.type == 'suggestion')
    }

    if (this.searchFilters.includes('error')) {
      typeFilters.push((s: ISummaryRow) => s.type == 'error')
    }

    this.filteredSummaryRows = this.summaryRows.filter(
      (s) =>
        (!readFilters.length || readFilters.some((f) => f(s))) &&
        (!typeFilters.length || typeFilters.some((f) => f(s)))
    )
  }

  addFilter(filterString: string): void {
    if (filterString && !this.searchFilters.includes(filterString)) {
      this.searchFilters.push(filterString)
    }
    this.applyFilters()
    this.registerAutoCompleteChange()
  }

  removeFilter(filterString: string): void {
    const index = this.searchFilters.indexOf(filterString)
    if (index >= 0) {
      this.searchFilters.splice(index, 1)
    }
    this.applyFilters()
  }

  toggleRead(item: ISummaryRow) {
    item.isRead = !item.isRead
    this.applyFilters()
  }

  registerAutoCompleteChange() {
    this.obsFilteredOptions = this.filterInput.valueChanges.pipe(
      startWith(''),
      map((value) => this.autoCompleteOnChangeFilter(value))
    )
  }

  autoCompleteOnChangeFilter(value: string): string[] {
    return this.options.filter((option) => option.toLowerCase().includes(value))
  }
  spannerTab() {
    this.clickEvent.setTabToSpanner()
  }
}
