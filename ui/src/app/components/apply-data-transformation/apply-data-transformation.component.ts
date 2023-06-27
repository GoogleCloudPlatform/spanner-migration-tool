import { Component, EventEmitter, Input, OnInit, Output, TRANSLATIONS } from '@angular/core';
import { FormBuilder, FormGroup, Validators } from '@angular/forms';
import { DataTypes, Dialect, Transformation } from 'src/app/app.constants';
import IConv from 'src/app/model/conv';
import IRule, { ITransformation } from 'src/app/model/rule';
import { ConversionService } from 'src/app/services/conversion/conversion.service';
import { DataService } from 'src/app/services/data/data.service';
import { SidenavService } from 'src/app/services/sidenav/sidenav.service';

@Component({
  selector: 'app-apply-data-transformation',
  templateUrl: './apply-data-transformation.component.html',
  styleUrls: ['./apply-data-transformation.component.scss']
})
export class ApplyDataTransformationComponent implements OnInit {
  @Input() ruleNameValid: boolean = false
  @Input() ruleName: string = ''
  @Input() ruleType: string = ''
  @Output() resetRuleType: EventEmitter<any> = new EventEmitter<any>()
  tableNames: string[] = []
  viewRuleData: IRule = {}
  viewRuleFlag: boolean = false
  conv: IConv = {} as IConv
  ruleId: any = ''
  applyDataTransformationForm: FormGroup
  functions: string[] = Transformation.Functions
  zeroOperandFunction: string[] = Transformation.ZeroOperandFunction
  unaryFunction: string[] = Transformation.UnaryFunction
  binaryFunction: string[] = Transformation.BinaryFunction
  selectedTableId: string = ''
  selectedFunction: string = ''
  selectedFirstInputType: string = ''
  selectedFirstInputValue: string = ''
  selectedSecondInputType: string = ''
  selectedSecondInputValue: any;
  selectedFirstStaticDataType: any;
  selectedSecondStaticDataType: any;
  selectedOperator: string = ''
  inputType: string[] = Transformation.InputType
  operatorArray: any[] = []
  variables: any[] = []
  spannerColumns: any[] = []
  sourceColumns: any[] = []
  tableDetails: any[] = []
  datatypes: string[] = []
  selectedOutputAction: string = ''
  selectedFilterAction: boolean = true
  selectedOutputColumn: string = ''
  selectedOutputDatatype: string = ''
  variablesMap: Map<string, any[]> = new Map();


  outputAction = [
    { value: 'filter', displayName: 'Filter' },
    { value: 'writeToColumn', displayName: 'Write to spanner column' },
    { value: 'writeToVar', displayName: 'Write to new variable' },
  ]

  filterOptionList = [
    { value: false, displayName: 'Skip the records' },
    { value: true, displayName: 'Save the records' },
  ]
  constructor(
    private fb: FormBuilder,
    private data: DataService,
    private sidenav: SidenavService,
    private conversion: ConversionService
  ) {
    this.applyDataTransformationForm = this.fb.group({
      tableName: ['', Validators.required],
      function: ['', Validators.required],
      firstInputType: [''],
      operator: [''],
      firstStaticDataType: [''],
      firstStaticValue: [''],
      firstColumnName: [''],
      firstVariableName: [''],
      secondInputType: [''],
      secondStaticDataType: [''],
      secondStaticValue: [''],
      secondColumnName: [''],
      secondVariableName: [''],
      outputAction: ['', Validators.required],
      filterAction: [''],
      outputColumn: [''],
      outputVarDatatype: [''],
      outputVarValue: [''],
    })
  }

  ngOnInit(): void {
    this.data.conv.subscribe({
      next: (res: IConv) => {
        this.conv = res
        Object.keys(res.SpSchema).forEach((tableId: any) => {
          this.tableDetails.push({ value: tableId, display: res.SpSchema[tableId].Name })
        });
      },
    })

    this.data.transformation.subscribe({
      next: (data: ITransformation[]) => {
        for (let i in data) {
          if (data[i].Action === 'writeToVar') {
            const tableId = data[i]?.AssociatedObjects;
            if (tableId !== undefined) {
              const varName = data[i].ActionConfig.varName.value;
              if (this.variablesMap.has(tableId)) {
                this.variablesMap.get(tableId)?.push({ display: varName, value: varName });
              } else {
                this.variablesMap.set(tableId, [{ display: varName, value: varName }]);
              }
            }
          }
        }
      },
    })

    if (this.conv.SpDialect == Dialect.GoogleStandardSQLDialect) {
      this.datatypes = DataTypes.GoogleStandardSQL
    } else {
      this.datatypes = DataTypes.PostgreSQL
    }

    this.sidenav.displayRuleFlag.subscribe((flag: boolean) => {
      this.viewRuleFlag = flag
      if (this.viewRuleFlag) {
        this.sidenav.ruleData.subscribe((data: ITransformation) => {
          this.viewRuleData = data
          if (this.viewRuleData && this.viewRuleFlag) {
            this.populateRuleData(this.viewRuleData)
          }
        })
      }
    })
  }

  populateRuleData(data: ITransformation) {
    this.ruleId = data?.Id
    this.applyDataTransformationForm.controls['tableName'].setValue(data.AssociatedObjects)
    if (data.AssociatedObjects !== undefined) {
      this.selectedTableChange(data.AssociatedObjects)
    }
    this.applyDataTransformationForm.controls['function'].setValue(data.Function)
    if (data.Action !== undefined) {
      this.selectedOutputAction = data.Action
    }
    this.populateActionConfig(data)
    this.populateInput(data)
    this.applyDataTransformationForm.disable()
  }

  populateInput(data: ITransformation) {
    if (data.Function !== undefined) {
      this.selectedFunction = data.Function
      this.onFunctionSelectionChange(this.selectedFunction)
    }
    if (this.zeroOperandFunction.indexOf(this.selectedFunction) === -1) {
      this.populateFirstInput(data.Input[0])
      if (this.binaryFunction.indexOf(this.selectedFunction) > -1) {
        this.selectedOperator = data.Input[1]?.value
        if (this.selectedFunction !== 'not') {
          this.populateSecondInput(data.Input[2])
        }
      }
    }
  }

  populateFirstInput(input: any) {
    this.selectedFirstInputType = input?.type
    if (this.selectedFirstInputType === 'source-column' || this.selectedFirstInputType === 'variable') {
      this.selectedFirstInputValue = input?.value
    } else if (this.selectedFirstInputType === 'static') {
      this.applyDataTransformationForm.controls['firstStaticValue'].setValue(input?.value)
      this.selectedFirstStaticDataType = input?.datatype
    }
  }

  populateSecondInput(input: any) {
    this.selectedSecondInputType = input?.type
    if (this.selectedSecondInputType === 'source-column' || this.selectedSecondInputType === 'variable') {
      this.selectedSecondInputType = input?.value
    } else if (this.selectedSecondInputType === 'static') {
      this.applyDataTransformationForm.controls['secondStaticValue'].setValue(input?.value)
      this.selectedSecondStaticDataType = input?.datatype
    }
  }

  populateActionConfig(data: ITransformation) {
    if (data.Action === 'filter') {
      if (data.ActionConfig.include !== undefined) {
        this.selectedFilterAction = data.ActionConfig.include === 'true'
      }
      this.applyDataTransformationForm.controls['filterAction'].setValue(this.selectedFilterAction)
    } else if (data.Action === 'writeToColumn') {
      if (data.ActionConfig.column !== undefined) {
        this.selectedOutputColumn = data.ActionConfig.column
      }
      this.applyDataTransformationForm.controls['outputColumn'].setValue(data.ActionConfig.column)
    } else if (data.Action === 'writeToVar') {
      if (data.ActionConfig.varName.datatype !== undefined) {
        this.selectedOutputDatatype = data.ActionConfig.varName.datatype
      }
      this.applyDataTransformationForm.controls['outputVarDatatype'].setValue(data.ActionConfig.varName.datatype)
      this.applyDataTransformationForm.controls['outputVarValue'].setValue(data.ActionConfig.varName.value)
    }
  }

  onOperatorChange() {
    if (this.selectedOperator !== 'not') {
      const secondInputTypeControl = this.applyDataTransformationForm.get('secondInputType');
      if (this.isSecondInputRequired()) {
        secondInputTypeControl?.setValidators([Validators.required]);
      } else {
        secondInputTypeControl?.clearValidators();
      }
      secondInputTypeControl?.updateValueAndValidity();
    }
  }

  onFunctionSelectionChange(functionName: string) {
    this.selectedFunction = functionName
    const index = this.inputType.indexOf('context');
    if (index > -1) {
      this.inputType.splice(index, 1);
    }

    switch (functionName) {
      case 'mathOp':
        this.operatorArray = Transformation.MathOperators
        break
      case 'logicalOp':
        this.operatorArray = Transformation.LogicalOperators
        break
      case 'compare':
        this.operatorArray = Transformation.CompareOperators
        break
      case 'noOp':
        this.inputType.push('context')
        break
    }
    const firstInputTypeControl = this.applyDataTransformationForm.get('firstInputType');
    if (this.zeroOperandFunction.indexOf(this.selectedFunction) === -1) { 
      firstInputTypeControl?.setValidators([Validators.required]);
      firstInputTypeControl?.updateValueAndValidity();
    } else {
      firstInputTypeControl?.clearValidators();
      firstInputTypeControl?.updateValueAndValidity();
    }
  }

  updateInputValidators(inputPrefix: string) {
    const inputType = this.applyDataTransformationForm.get(`${inputPrefix}InputType`)?.value;
    const staticValueControl = this.applyDataTransformationForm.get(`${inputPrefix}StaticValue`);
    const staticDatatypeControl = this.applyDataTransformationForm.get(`${inputPrefix}StaticDataType`);
    const columnNameControl = this.applyDataTransformationForm.get(`${inputPrefix}ColumnName`);
    const variableNameControl = this.applyDataTransformationForm.get(`${inputPrefix}VariableName`);

    staticDatatypeControl?.clearValidators();
    staticValueControl?.clearValidators();
    columnNameControl?.clearValidators();
    variableNameControl?.clearValidators();

    if (inputType === 'static') {
      staticValueControl?.setValidators(Validators.required);
      staticDatatypeControl?.setValidators(Validators.required);
    } else if (inputType === 'source-column') {
      columnNameControl?.setValidators(Validators.required);
    } else if (inputType === 'variable') {
      variableNameControl?.setValidators(Validators.required);
    }

    staticValueControl?.updateValueAndValidity();
    staticDatatypeControl?.updateValueAndValidity();
    columnNameControl?.updateValueAndValidity();
    variableNameControl?.updateValueAndValidity();
  }

  // Function to add dynamic validators based on selectedOutputAction
  addValidators() {
    const filterActionControl = this.applyDataTransformationForm.get('filterAction');
    const outputColumnControl = this.applyDataTransformationForm.get('outputColumn');
    const outputVarDatatypeControl = this.applyDataTransformationForm.get('outputVarDatatype');
    const outputVarValueControl = this.applyDataTransformationForm.get('outputVarValue');

    // Reset validators for all controls
    filterActionControl?.clearValidators();
    outputColumnControl?.clearValidators();
    outputVarDatatypeControl?.clearValidators();
    outputVarValueControl?.clearValidators();

    if (this.selectedOutputAction === 'filter') {
      // Add required validator for filterActionControl
      filterActionControl?.setValidators([Validators.required]);
    } else if (this.selectedOutputAction === 'writeToColumn') {
      // Add required validator for outputColumnControl
      outputColumnControl?.setValidators([Validators.required]);
    } else if (this.selectedOutputAction === 'writeToVar') {
      // Add required validators for outputVarDatatypeControl and outputVarValueControl
      outputVarDatatypeControl?.setValidators([Validators.required]);
      outputVarValueControl?.setValidators([Validators.required]);
    }

    // Update validators for all controls
    filterActionControl?.updateValueAndValidity();
    outputColumnControl?.updateValueAndValidity();
    outputVarDatatypeControl?.updateValueAndValidity();
    outputVarValueControl?.updateValueAndValidity();
  }

  selectedTableChange(tableId: string) {
    this.selectedTableId = tableId
    this.spannerColumns = []
    this.sourceColumns = []
    Object.keys(this.conv.SpSchema[tableId].ColDefs).forEach((colId: any) => {
      this.spannerColumns.push({ value: colId, display: this.conv.SpSchema[tableId].ColDefs[colId].Name })
    });
    Object.keys(this.conv.SrcSchema[tableId].ColDefs).forEach((colId: any) => {
      this.sourceColumns.push({ value: colId, display: this.conv.SrcSchema[tableId].ColDefs[colId].Name })
    });
  }

  applyDataTransformation() {
    this.applyRule()
    this.resetRuleType.emit('')
    this.sidenav.closeSidenav()
  }

  applyRule() {
    let tableId: string = this.selectedTableId
    let actionConfig = this.getActionConfig()
    let input = this.getInput()
    let payload: ITransformation = {
      Name: this.ruleName,
      Type: 'apply_data_transformation',
      ObjectType: 'Table',
      AssociatedObjects: tableId,
      Enabled: true,
      Id: '',
      Action: this.selectedOutputAction,
      ActionConfig: actionConfig,
      Input: input,
      Function: this.selectedFunction,
    }
    this.data.applyDataTransformation(payload)
  }

  getInput(): any[] {
    let input: any[] = []
    if (this.zeroOperandFunction.indexOf(this.selectedFunction) === -1) {
      if (this.selectedFirstInputType === 'context') {
        input.push('{ "type": "context" }')
      } else if (this.selectedFirstInputType === 'source-column' || this.selectedFirstInputType === 'variable') {
        input.push({ "type": this.selectedFirstInputType, "value": this.selectedFirstInputValue })
      } else {
        input.push({ "type": this.selectedFirstInputType, "datatype": this.selectedFirstStaticDataType, "value": this.applyDataTransformationForm.value.firstStaticValue })
      }
    }
    if (this.binaryFunction.indexOf(this.selectedFunction) > -1) {
      input.push({ "type": "operator", "value": this.selectedOperator })
      if (this.selectedOperator !== 'not') {
        if (this.selectedSecondInputType === 'context') {
          input.push({ "type": "context" })
        } else if (this.selectedSecondInputType === 'source-column' || this.selectedSecondInputType === 'variable') {
          input.push({ "type": this.selectedSecondInputType, "value": this.selectedSecondInputValue })
        } else {
          input.push({ "type": this.selectedSecondInputType, "datatype": this.selectedSecondStaticDataType, "value": this.applyDataTransformationForm.value.secondStaticValue })
        }
      }
    }
    return input
  }

  getActionConfig(): string {
    let actionConfig: any
    if (this.selectedOutputAction === 'filter') {
      actionConfig = {
        "include": this.selectedFilterAction.toString()
      }
    } else if (this.selectedOutputAction === 'writeToColumn') {
      actionConfig = {
        "column": this.selectedOutputColumn
      }
    } else if (this.selectedOutputAction === 'writeToVar') {
      actionConfig = {
        "varName": { "datatype": this.selectedOutputDatatype, "value": this.applyDataTransformationForm.value.outputVarValue }
      }
    }
    return actionConfig
  }

  getInputOptions(inputType: string) {
    if (inputType === 'source-column') {
      return this.sourceColumns;
    } else if (inputType === 'variable') {
      return this.variablesMap.get(this.selectedTableId);
    }
    return [];
  }

  isSecondInputRequired() {
    return this.binaryFunction.indexOf(this.selectedFunction) != -1 && this.selectedOperator === 'not';
  }

  deleteRule() {
    this.data.dropTransformation(this.ruleId)
    this.resetRuleType.emit('')
    this.sidenav.closeSidenav()
  }

}
