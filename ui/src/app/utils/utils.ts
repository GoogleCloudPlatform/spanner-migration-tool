import { SourceDbNames } from '../app.constants'
import IConv from '../model/conv'

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

export function downloadSession(conv: IConv) {
  var a = document.createElement('a')
    // JS automatically converts the input (64bit INT) to '9223372036854776000' during conversion as this is the max value in JS.
    // However the max value received from server is '9223372036854775807'
    // Therefore an explicit replacement is necessary in the JSON content in the file.
    let resJson = JSON.stringify(conv).replace(/9223372036854776000/g, '9223372036854775807')
    a.href = 'data:text/json;charset=utf-8,' + encodeURIComponent(resJson)
    a.download = `${conv.SessionName}_${conv.DatabaseType}_${conv.DatabaseName}.json`
    a.click()
}
