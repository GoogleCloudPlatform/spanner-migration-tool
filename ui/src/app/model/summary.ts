export default interface ISummary {
  SrcTable: string
  SpTable: string
  Warnings: string[]
  Notes: string[]
  WarningsCount: number
  NotesCount: number
}
export interface ISummaryRow {
  type: string
  content: string
  isRead: boolean
}
