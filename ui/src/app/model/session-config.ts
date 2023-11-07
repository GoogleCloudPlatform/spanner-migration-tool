export default interface ISessionConfig {
  driver: string | null | undefined
  filePath: string | null | undefined
  dbName?: string
  createdAt?: string
}
