import { Component, OnInit} from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { MatDialogRef} from '@angular/material/dialog';
import ICreateSequence from 'src/app/model/auto-gen';
import { DataService } from 'src/app/services/data/data.service';
import { FetchService } from 'src/app/services/fetch/fetch.service'
import { linkedFieldsValidatorSequence } from 'src/app/utils/utils';
@Component({
  selector: 'app-add-new-sequence',
  templateUrl: './add-new-sequence.component.html',
  styleUrls: ['./add-new-sequence.component.scss']
})

export class AddNewSequenceComponent implements OnInit {
  addNewSequenceForm: FormGroup
  selectedSequenceKind: string = ""
  sequenceKinds : string[] = []
  constructor(
    private formBuilder: FormBuilder,
    private dataService: DataService,
    private fetchSerice: FetchService,
    private dialogRef: MatDialogRef<AddNewSequenceComponent>) {
    this.addNewSequenceForm = this.formBuilder.group({
      name: ['', [Validators.required, Validators.minLength(1), Validators.maxLength(128), Validators.pattern('^[a-zA-Z][a-zA-Z0-9_]*$')]],
      sequenceKind: ['', Validators.required],
      skipRangeMin: ['', Validators.pattern('^[0-9]+$')],
      skipRangeMax: ['', Validators.pattern('^[0-9]+$')],
      startWithCounter: ['', Validators.pattern('^[0-9]+$')],
    }, { validator: linkedFieldsValidatorSequence('skipRangeMin', 'skipRangeMax') })
    this.fetchSerice.getSequenceKind().subscribe(
      (sequenceKinds: any) => {
        this.sequenceKinds = sequenceKinds;
      }
    );
  }
  
  ngOnInit(): void {}
 
  addNewSequence() {
    let formValue = this.addNewSequenceForm.value
    let payload: ICreateSequence = {
      Name: formValue.name,
      SequenceKind: this.selectedSequenceKind,
      SkipRangeMin: formValue.skipRangeMin,
      SkipRangeMax: formValue.skipRangeMax,
      StartWithCounter: formValue.startWithCounter
    }
    this.dataService.addSequence(payload)
    this.dialogRef.close()
  }
}
