import ITargetDetails from "./target-details";

export default interface IMigrationDetails {
    TargetDetails: ITargetDetails
    MigrationType: string
    MigrationMode: string
}

export interface IProgress {
    Progress: number
    ErrorMessage: string
    ProgressStatus: number
}

export interface IGeneratedResources {
    DatabaseName: string
    DatabaseUrl: string
    BucketName: string
    BucketUrl: string
    DataStreamJobName: string
    DataStreamJobUrl: string
    DataflowJobName: string
    DataflowJobUrl: string
}