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

export interface IGcsConfig {
    ttlInDays: string
    ttlInDaysSet: boolean
}