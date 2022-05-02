import { Component, Input, OnInit, Output, SimpleChanges, EventEmitter } from '@angular/core'
import { COMMA, ENTER, T } from '@angular/cdk/keycodes'
import { MatChipInputEvent } from '@angular/material/chips'
import { DataService } from 'src/app/services/data/data.service'
import ISummary from 'src/app/model/Summary'
import { FlatNode } from 'src/app/model/SchemaObjectNode'
import { Observable, of } from 'rxjs'
import { map, startWith, debounceTime } from 'rxjs/operators'
import { FormControl } from '@angular/forms'

@Component({
  selector: 'app-summary',
  templateUrl: './summary.component.html',
  styleUrls: ['./summary.component.scss'],
})
export class SummaryComponent implements OnInit {
  @Output() changeIssuesLabel: EventEmitter<number> = new EventEmitter<number>()
  summaryRows: SummaryRow[] = []
  summary: Map<string, ISummary> = new Map<string, ISummary>()
  filteredSummaryRows: SummaryRow[] = []
  readonly separatorKeysCodes = [] as const
  summaryCount: number = 0

  filterInput = new FormControl()
  options: string[] = ['read', 'unread', 'warning', 'suggestion', 'note']
  obsFilteredOptions: Observable<string[]> = new Observable<string[]>()
  searchFilters: string[] = ['unread', 'warning', 'note']

  @Input() currentObject: FlatNode | null = null
  constructor(private data: DataService) {}

  ngOnInit(): void {
    this.data.summary.subscribe({
      next: (summary: Map<string, ISummary>) => {
        this.summary = summary
        if (this.currentObject) {
          let s = summary.get(this.currentObject.name)
          if (s) {
            this.initiateSummaryCollection(s)
            this.summaryCount = s.NotesCount + s.WarningsCount
            this.changeIssuesLabel.emit(s.NotesCount + s.WarningsCount)
          }
        }
      },
    })

    this.registerAutoCompleteChange()
  }

  ngOnChanges(changes: SimpleChanges): void {
    this.currentObject = changes?.['currentObject']?.currentValue || this.currentObject
    this.summaryRows = []
    if (this.currentObject) {
      let s = this.summary.get(this.currentObject.name)
      if (s) {
        this.initiateSummaryCollection(s)
        this.summaryCount = s.NotesCount + s.WarningsCount
        this.changeIssuesLabel.emit(s.NotesCount + s.WarningsCount)
      }
    }
  }

  initiateSummaryCollection(summary: ISummary) {
    summary.Warnings.forEach((v) => {
      this.summaryRows.push({
        type: 'warning',
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
    this.filteredSummaryRows = this.summaryRows
  }

  applyFilters() {
    let typeFilters: Array<(data: SummaryRow) => Boolean> = []
    let readFilters: Array<(data: SummaryRow) => Boolean> = []

    if (this.searchFilters.includes('read')) {
      readFilters.push((s: SummaryRow) => s.isRead)
    }
    if (this.searchFilters.includes('unread')) {
      readFilters.push((s: SummaryRow) => !s.isRead)
    }

    if (this.searchFilters.includes('warning')) {
      typeFilters.push((s: SummaryRow) => s.type == 'warning')
    }
    if (this.searchFilters.includes('note')) {
      typeFilters.push((s: SummaryRow) => s.type == 'note')
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

  toggleRead(item: SummaryRow) {
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
}

export interface SummaryRow {
  type: string
  content: string
  isRead: boolean
}
