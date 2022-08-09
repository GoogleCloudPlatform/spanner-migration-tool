import ITargetDetails from "./target-details";

export default interface IMigrationDetails {
    TargetDetails: ITargetDetails
    MigrationType: string
    MigrationMode: string
}

export interface Progress {
    progress: number
    error: string
}