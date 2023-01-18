export default interface ITargetDetails {
    TargetDB: string
    SourceConnProfile: string
    TargetConnProfile: string
    ReplicationSlot: string
    Publication: string
}

export interface ISpannerDetails {
    Dialect: string
}