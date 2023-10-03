import { IDataflowConfig, IDataprocConfig } from "./profile";
import ITargetDetails from "./target-details";

export default interface IMigrationDetails {
    TargetDetails: ITargetDetails
    DataflowConfig: IDataflowConfig
    DataprocConfig: IDataprocConfig
    MigrationType: string
    MigrationMode: string
    IsSharded: boolean
    skipForeignKeys: boolean
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
    ShardToDatastreamMap: Map<string, ResourceDetails>
    ShardToDataflowMap: Map<string, ResourceDetails>
}

export interface ResourceDetails {
    JobName: string
    JobUrl: string
}

export interface IDataprocJobs {
    SrcTable: string[]
    DataprocJobIds: string[]
    DataprocJobUrls: string[]
    DataprocJobStatus: string[]
}

export interface ISourceAndTargetDetails {
    SpannerDatabaseName: string
    SpannerDatabaseUrl: string
    SourceDatabaseName: string
    SourceDatabaseType: string
}

export interface ITables {
    TableList: string[]
}

export interface ITableState {
    TableName: string
    TableId: string
    isDeleted: boolean
}