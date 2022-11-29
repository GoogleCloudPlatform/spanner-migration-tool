export default interface ISession {
  VersionId: string
  SessionName: string
  EditorName: string
  DatabaseType: string
  DatabaseName: string
  Notes: string[]
  CreateTimestamp: string[]
}

export interface ISaveSessionPayload {
  SessionName: string
  EditorName: string
  DatabaseName: string
  Notes?: string[]
}
