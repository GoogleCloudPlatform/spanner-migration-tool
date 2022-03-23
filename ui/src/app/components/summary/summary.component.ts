import { Component, OnInit } from '@angular/core'
import { COMMA, ENTER } from '@angular/cdk/keycodes'
import { MatChipInputEvent } from '@angular/material/chips'

@Component({
  selector: 'app-summary',
  templateUrl: './summary.component.html',
  styleUrls: ['./summary.component.scss'],
})
export class SummaryComponent implements OnInit {
  constructor() {}
  ngOnInit(): void {
    this.initiateSummaryCollection()
  }

  addOnBlur = true
  readonly separatorKeysCodes = [ENTER, COMMA] as const
  filterStrings : string[] = ["Read", "Unread"]
  summaryItems : SummaryItem[] =[]

  add(event: MatChipInputEvent): void {
    const value = (event.value || '').trim()
    if (value) {
      this.filterStrings.push(value)
    }
    event.chipInput!.clear()
  }

  remove(filterString: string): void {
    const index = this.filterStrings.indexOf(filterString)
    if (index >= 0) {
      this.filterStrings.splice(index, 1)
    }
  }

  initiateSummaryCollection(){
    this.summaryItems.push({
      type: 'issue',
      content:
        'Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.',
      isRead: false,
    })
    this.summaryItems.push({
      type: 'warning',
      content:
        'Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum.',
      isRead: false,
    })
    this.summaryItems.push({
      type: 'suggestion',
      content:
        'Sed ut perspiciatis unde omnis iste natus error sit voluptatem accusantium doloremque laudantium, totam rem aperiam, eaque ipsa quae ab illo inventore veritatis et quasi architecto beatae vitae dicta sunt explicabo. Nemo enim ipsam voluptatem quia voluptas sit aspernatur aut odit aut fugit, sed quia consequuntur magni dolores eos qui ratione voluptatem sequi nesciunt.',
      isRead: true,
    })
  }
}

export interface SummaryItem {
  type: string
  content: string
  isRead: boolean
}
