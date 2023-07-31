export default interface ISummary {
  SrcTable: string
  SpTable: string
  Errors: Issue[]
  Warnings: Issue[]
  Suggestions: Issue[]
  Notes: Issue[]
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

export interface Issue {
	issueType: string
	description: string
}
