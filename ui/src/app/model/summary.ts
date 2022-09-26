export default interface ISummary {
  SrcTable: string
  SpTable: string
  Errors: string[]
  Warnings: string[]
  Suggestions: string[]
  Notes: string[]
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
