import { Component, Inject, OnInit } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { MatDialogRef, MAT_DIALOG_DATA } from '@angular/material/dialog';
import { ColLength, Dialect } from 'src/app/app.constants';
import { IColumn, ISpannerDetails } from 'src/app/model/conv';
import { IAddColumn } from 'src/app/model/update-table';
import { DataService } from 'src/app/services/data/data.service';
import { TargetDetailsFormComponent } from '../target-details-form/target-details-form.component';
interface IAddColumnProps {
  dialect: string
  tableId: string
}
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
  constructor(
    private formBuilder: FormBuilder,
    private dataService: DataService,
    private dialogRef: MatDialogRef<TargetDetailsFormComponent>,
    @Inject(MAT_DIALOG_DATA) public data: IAddColumnProps) {
    this.dialect = data.dialect
    this.tableId = data.tableId
    this.addNewColumnForm = this.formBuilder.group({
      name: ['', [Validators.required, Validators.minLength(1), Validators.maxLength(128), Validators.pattern('^[a-zA-Z][a-zA-Z0-9_]*$')]],
      datatype: [],
      length: ['',Validators.pattern('^[0-9]+$')],
      isNullable: [],
    })
  }


  isNullable = [
    { value: false, displayName: 'No' },
    { value: true, displayName: 'Yes' },
  ]

  ngOnInit(): void {
    if (this.dialect == Dialect.GoogleStandardSQLDialect) {
      this.datatypes = ['BOOL','BYTES','DATE','FLOAT64','INT64','STRING', 'TIMESTAMP', 'NUMERIC', 'JSON']
    } else {
      this.datatypes = ['BOOL','BYTEA','DATE','FLOAT8','INT8','VARCHAR', 'TIMESTAMPTZ', 'NUMERIC', 'JSONB']
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
      Length: parseInt(formValue.length),
      IsNullable: this.selectedNull
    }
    this.dataService.addColumn(this.tableId, payload)
    this.dialogRef.close()
  }
}