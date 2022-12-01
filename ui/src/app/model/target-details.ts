export default interface ITargetDetails {
    TargetDB: string
    Dialect: string
    SourceConnProfile: string
    TargetConnProfile: string
    ReplicationSlot: string
    Publication: string
}