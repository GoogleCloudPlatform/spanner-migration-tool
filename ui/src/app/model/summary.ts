export default interface ISummary {
  SrcTable: string
  SpTable: string
  Errors: IssueClassified[]
  Warnings: IssueClassified[]
  Suggestions: IssueClassified[]
  Notes: IssueClassified[]
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

export interface IssueClassified {
	issueType: string
	description: string
}
