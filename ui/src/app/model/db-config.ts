export default interface IDbConfig {
  dbEngine: string| null | undefined
  isSharded: boolean| null | undefined
  hostName: string | null | undefined
  port: string | null | undefined
  userName: string | null | undefined
  password: string | null | undefined
  dbName: string | null | undefined
  shardId?: string
}

export interface IDbConfigs {
  dbConfigs: Array<IDbConfig>
  isRestoredSession: string
}

export interface IShardSessionDetails {
  sourceDatabaseEngine: string
  isRestoredSession: string
}