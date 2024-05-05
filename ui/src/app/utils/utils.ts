import { AbstractControl, FormGroup, ValidationErrors, ValidatorFn } from '@angular/forms';
import { SourceDbNames } from '../app.constants'
import IConv from '../model/conv'
import { AutoGen } from '../model/edit-table'

export interface GroupedAutoGens {
  [key: string]: { [type: string]: AutoGen[] };
}


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

export function groupAutoGenByType(autoGens: AutoGen[]): { [type: string]: AutoGen[] } {
  return autoGens.reduce((acc: { [type: string]: AutoGen[] }, autoGen: AutoGen) => {
    const type = autoGen.GenerationType;
    if (!acc[type]) {
      acc[type] = [];
    }
    acc[type].push(autoGen);
    return acc;
  }, {});
}

export function processAutoGens(autoGenMap: any): GroupedAutoGens {
  const groupedAutoGens: GroupedAutoGens = {};
  Object.keys(autoGenMap).forEach(key => {
    groupedAutoGens[key] = groupAutoGenByType(autoGenMap[key]);
  });
  return groupedAutoGens;
}

export function linkedFieldsValidator(skipRangeMin: string, skipRangeMax: string): ValidatorFn {
  return (control: AbstractControl): ValidationErrors | null => {
    const formGroup = control as FormGroup; 
    const min = formGroup.get(skipRangeMin);
    const max = formGroup.get(skipRangeMax);

    if (!min || !max) {
      return null; 
    }
    if ((min.value && !max.value) || (!min.value && max.value)) {
      return { linkedError: 'Both Skip Range Min and Max are required' };
    }
    return null; 
  };
}