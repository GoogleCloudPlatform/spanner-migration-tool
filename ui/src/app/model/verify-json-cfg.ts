export default interface IVerifyJsonDetails {
    json: string
}

export interface IResourcesGenerated {
    ResourcesCreated: Map<string,IShardResourcesGenerated>
}

export interface IShardResourcesGenerated {
    sourceConnectionProfile : boolean
    targetConnectionProfile : boolean
}