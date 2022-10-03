export default interface IConnectionProfile{
    DisplayName: string
    Name: string
}

export interface ICreateConnectionProfile{
    Id: string
    Region: string
    ValidateOnly: boolean
    IsSource: boolean
    Bucket: string
}