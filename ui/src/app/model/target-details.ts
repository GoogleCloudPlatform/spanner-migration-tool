export default interface ITargetDetails {
    TargetDB: string
    SourceConnProfile: string
    TargetConnProfile: string
    ReplicationSlot: string
    Publication: string
    GcsMetadataPath: GcsMetadataPath
}

export interface GcsMetadataPath {
    GcsBucketName : string
    GcsBucketRootPath: string
}

export interface ISpannerDetails {
    Dialect: string
}