export default interface IIssueReport {
    errors: Map<string, IssueDescription>
    warnings: Map<string, IssueDescription>
    suggestions: Map<string, IssueDescription>
    notes: Map<string, IssueDescription>
}

export interface IssueDescription {
    tableCount: number
    tableNames: Set<string>
}

