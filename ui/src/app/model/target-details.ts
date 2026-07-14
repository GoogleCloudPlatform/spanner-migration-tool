export default interface ITargetDetails {
    TargetDB: string
    GcsMetadataPath: GcsMetadataPath
    DefaultTimezone: string
}

export interface GcsMetadataPath {
    GcsBucketName : string
    GcsBucketRootPath: string
}

export interface ISpannerDetails {
    Dialect: string
}