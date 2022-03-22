export default interface ISession {
  VersionId: string
  SessionName: string
  EditorName: string
  DatabaseType: string
  DatabaseName: string
  Notes: string[]
  CreatedOn: string[]
}

export interface ISaveSessionPayload {
  SessionName: string
  EditorName: string
  DatabaseType: string
  DatabaseName: string
  Notes: string[]
  Tags: string[]
}
