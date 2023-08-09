export default interface IIssueReport {
    errors: Map<string, TablesInformation>
    warnings: Map<string, TablesInformation>
    suggestions: Map<string, TablesInformation>
    notes: Map<string, TablesInformation>
}

export interface TablesInformation {
    tableCount: number
    tableNames: Set<string>
}

