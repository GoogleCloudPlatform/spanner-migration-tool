import ITargetDetails from "./target-details";

export default interface IMigrationDetails {
    TargetDetails: ITargetDetails
    MigrationType: string
    MigrationMode: string
}

export interface IProgress {
    Progress: number
    ErrorMessage: string
    Message: string
}