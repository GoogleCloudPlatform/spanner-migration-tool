export default interface IConnectionProfile {
    DisplayName: string
    Name: string
}

export interface ICreateConnectionProfile {
    Id: string
    ValidateOnly: boolean
    IsSource: boolean
}

export interface ICreateConnectionProfileV2 {
    Id: string
    ValidateOnly: boolean
    IsSource: boolean
    Host?: string,
    Port?: string,
    User?: string,
    Password?: string
}

export interface ISetUpConnectionProfile {
    IsSource: boolean
    SourceDatabaseType: string
}

export interface IShardedDataflowMigration {
    IsSource: boolean
    SourceDatabaseType: string
    Region: string
}

export interface IDataflowConfig {
    network: string
    subnetwork: string
    vpcHostProjectId: string
    maxWorkers: string
    numWorkers: string
    serviceAccountEmail: string
    machineType: string
    additionalUserLabels: string
    kmsKeyName: string
    projectId: string
    location: string
    gcsTemplatePath: string
}

export interface IDirectConnectionConfig {
    host: string | null | undefined
    user: string | null | undefined
    password: string | null | undefined
    port: string | null | undefined
    dbName: string | null | undefined
}

export interface IDatastreamConnProfile {
    name: string
    location?: string
}

export interface ILogicalShard {
    dbName: string
    databaseId: string
    refDataShardId: string
}


export interface IDataShard {
    dataShardId: string
    srcConnectionProfile: IDatastreamConnProfile
    dstConnectionProfile: IDatastreamConnProfile
    streamLocation: string
    databases: Array<ILogicalShard>
}

export interface IShardConfigurationDataflow {
    schemaSource: IDirectConnectionConfig
    dataShards: Array<IDataShard>
}

export interface IMigrationProfile {
    configType: string
    shardConfigurationDataflow: IShardConfigurationDataflow
}