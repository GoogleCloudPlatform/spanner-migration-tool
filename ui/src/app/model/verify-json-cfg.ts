export default interface IVerifyJsonDetails {
    json: string
}

export interface IResourcesGenerated {
    ResourcesCreatedMap : Map<string,IShardResourcesGenerated>
}

export interface IShardResourcesGenerated {
    sourceConnectionProfile : boolean
    targetConnectionProfile : boolean
}

export interface ResourceGenerated {
    ShardId :                   string
    SourceConnectionProfile :   boolean
    TargetConnectionProfile :   boolean
}