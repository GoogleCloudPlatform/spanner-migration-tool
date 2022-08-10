import { SourceDbNames } from '../app.constants'

export function extractSourceDbName(srcDbName: string) {
  if (srcDbName == 'mysql' || srcDbName == 'mysqldump') {
    return SourceDbNames.MySQL
  }
  if (srcDbName === 'postgres' || srcDbName === 'pgdump' || srcDbName === 'pg_dump') {
    return SourceDbNames.Postgres
  }
  if (srcDbName === 'oracle') {
    return SourceDbNames.Oracle
  }
  if (srcDbName === 'sqlserver') {
    return SourceDbNames.SQLServer
  }
  return srcDbName
}
