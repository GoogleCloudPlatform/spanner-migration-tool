export default interface IConnectionProfile{
    DisplayName: string
    Name: string
}

export interface ICreateConnectionProfile{
    Id: string
    ValidateOnly: boolean
    IsSource: boolean
}