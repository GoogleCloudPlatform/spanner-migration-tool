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
  ngOnInit(): void {}

  addOnBlur = true
  readonly separatorKeysCodes = [ENTER, COMMA] as const
  tables: Table[] = [{ name: 'Order' }]

  add(event: MatChipInputEvent): void {
    const value = (event.value || '').trim()

    // Add our fruit
    if (value) {
      this.tables.push({ name: value })
    }

    // Clear the input value
    event.chipInput!.clear()
  }

  remove(fruit: Table): void {
    const index = this.tables.indexOf(fruit)

    if (index >= 0) {
      this.tables.splice(index, 1)
    }
  }
}

export interface Table {
  name: string
}
