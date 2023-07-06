export default interface IDbConfig {
  dbEngine: string
  isSharded: boolean
  hostName: string
  port: string
  userName: string
  password: string
  dbName: string
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