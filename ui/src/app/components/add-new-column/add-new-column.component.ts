import { Component, Inject, OnInit, Input } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { ColLength, DataTypes, Dialect, SourceDbNames, SpannerToCassandra, StorageKeys } from 'src/app/app.constants';
import { AutoGen, IAddColumnProps } from 'src/app/model/edit-table';
import { IAddColumn } from 'src/app/model/update-table';
import { DataService } from 'src/app/services/data/data.service';
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { GroupedAutoGens, processAutoGens } from 'src/app/utils/utils';
@Component({
  selector: 'app-add-new-column',
  templateUrl: './add-new-column.component.html',
  styleUrls: ['./add-new-column.component.scss']
})

export class AddNewColumnComponent implements OnInit {
  dialect: string = ""
  datatypes: string[] = []
  addNewColumnForm: FormGroup
  selectedDatatype: string = ""
  tableId: string = ""
  selectedNull: boolean = true
  dataTypesWithColLen: string[] = ColLength.DataTypes
  autoGenMap : any = {}
  selectedAutoGen: AutoGen = {
    Name: '',
    GenerationType: ''
  }
  processedAutoGenMap: GroupedAutoGens = {};
  srcDbName: string = localStorage.getItem(StorageKeys.SourceDbName) as string
  autoGenSupportedDbs: string[] = ['MySQL']
  autGenSupported: boolean = false
  constructor(
    private formBuilder: FormBuilder,
    private dataService: DataService,
    private fetchSerice: FetchService,
    private dialogRef: MatDialogRef<AddNewColumnComponent>,
    @Inject(MAT_DIALOG_DATA) public data: IAddColumnProps) {
    this.dialect = data.dialect
    this.tableId = data.tableId
    this.addNewColumnForm = this.formBuilder.group({
      name: ['', [Validators.required, Validators.minLength(1), Validators.maxLength(128), Validators.pattern('^[a-zA-Z][a-zA-Z0-9_]*$')]],
      datatype: ['', Validators.required],
      option: [''],
      length: ['',Validators.pattern('^[0-9]+$')],
      isNullable: [],
      autoGen: [{
        Name: "",
        GenerationType: ""
      }],
    })
    this.addNewColumnForm.get('datatype')?.valueChanges.subscribe((spannerType) => {
      this.updateValidatorsAndOptions(spannerType)
    })
    this.fetchSerice.getAutoGenMap().subscribe(
      (autoGen: any) => {
        this.autoGenMap = autoGen;
        this.processedAutoGenMap = processAutoGens(this.autoGenMap)
      }
    );
    this.autGenSupported = this.autoGenSupportedDbs.includes(this.srcDbName)
  }

  cassandraOptions: string[] = []

  isColumnNullable = [
    { value: false, displayName: 'No' },
    { value: true, displayName: 'Yes' },
  ]

  ngOnInit(): void {
    if (this.dialect == Dialect.GoogleStandardSQLDialect) {
      this.datatypes = DataTypes.GoogleStandardSQL
    } else {
      this.datatypes = DataTypes.PostgreSQL
    }
    if (this.srcDbName === SourceDbNames.Cassandra) {
      this.datatypes = this.datatypes.filter((type) => type !== 'JSON')
    }
  }

  updateValidatorsAndOptions(selectedDatatype: string) {
    const optionControl = this.addNewColumnForm.get('option')
    if (this.srcDbName === SourceDbNames.Cassandra) {
      this.cassandraOptions = SpannerToCassandra[selectedDatatype] || []
      if (this.cassandraOptions.length > 0) {
        optionControl?.setValue(this.cassandraOptions[0])
        optionControl?.setValidators([Validators.required])
      } else {
        optionControl?.clearValidators()
        optionControl?.setValue('')
      }
      optionControl?.updateValueAndValidity()
    }
  }

  changeValidator() {
    this.addNewColumnForm.controls['length'].clearValidators()
    if (this.selectedDatatype === 'BYTES') {
      this.addNewColumnForm.get('length')?.addValidators([Validators.required, Validators.max(ColLength.ByteMaxLength)])
    } else if(this.selectedDatatype === 'VARCHAR' || this.selectedDatatype === 'STRING') {
      this.addNewColumnForm.get('length')?.addValidators([Validators.required, Validators.max(ColLength.StringMaxLength)])
    }
    this.addNewColumnForm.controls['length'].updateValueAndValidity()
  }

  addNewColumn() {
    let formValue = this.addNewColumnForm.value
    let payload: IAddColumn = {
      Name: formValue.name,
      Datatype: this.selectedDatatype,
      Option: formValue.option,
      Length: parseInt(formValue.length),
      IsNullable: this.selectedNull,
      AutoGen: this.selectedAutoGen
    }
    this.dataService.addColumn(this.tableId, payload)
    this.dialogRef.close()
  }
}