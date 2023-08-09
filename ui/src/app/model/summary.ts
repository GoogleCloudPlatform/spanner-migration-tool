import { IIssue } from './structured-report'

export default interface ISummary {
  SrcTable: string
  SpTable: string
  Errors: IIssue[]
  Warnings: IIssue[]
  Suggestions: IIssue[]
  Notes: IIssue[]
  ErrorsCount: number
  WarningsCount: number
  SuggestionsCount: number
  NotesCount: number
}
export interface ISummaryRow {
  type: string
  content: string
  isRead: boolean
}
