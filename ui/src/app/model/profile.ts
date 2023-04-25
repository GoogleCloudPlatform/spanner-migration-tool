export default interface IConnectionProfile{
    DisplayName: string
    Name: string
}

export interface ICreateConnectionProfile{
    Id: string
    ValidateOnly: boolean
    IsSource: boolean
}

export interface ISetUpConnectionProfile{
    IsSource: boolean
    SourceDatabaseType: string
}

export interface IDataflowConfig{
    Network: string
    Subnetwork: string
    HostProjectId: string
}

export interface IDataprocConfig{
    Subnetwork: string
    Hostname: string
    Port: string
}