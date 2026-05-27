import { IGcsConfig } from "./profile";
import ITargetDetails from "./target-details";

export default interface IMigrationDetails {
    TargetDetails: ITargetDetails
    GcsConfig: IGcsConfig
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
    MigrationJobId: string
    DatabaseName: string
    DatabaseUrl: string
    BucketName: string
    BucketUrl: string
    MonitoringDashboardName: string
    MonitoringDashboardUrl: string
    AggMonitoringDashboardName: string
    AggMonitoringDashboardUrl: string
    ShardToShardResourcesMap: Map<string, ResourceDetails[]>
}

export interface ResourceDetails {
    DataShardId: string
    ResourceType: string
    ResourceName: string
    ResourceUrl: string
    GcloudCmd: string
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
