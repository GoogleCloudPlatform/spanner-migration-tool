// Copyright 2023 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transformation

import (
	"fmt"
	"math"
	"math/bits"
	"reflect"
	"sort"
	"time"

	"cloud.google.com/go/civil"
	"github.com/cloudspannerecosystem/harbourbridge/common/constants"
	"github.com/cloudspannerecosystem/harbourbridge/internal"
	"github.com/cloudspannerecosystem/harbourbridge/sources/common"
	"github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"
	"github.com/google/uuid"
)

type inputValue struct {
	value     interface{}
	inputType string
	dataType  string
}

type variable struct {
	value    interface{}
	dataType string
}

type input int

const (
	operator input = iota
	operand
)

func ProcessDataTransformatioNew(conv *internal.Conv, tableId string, cvtCols []string, cvtVals []interface{}, mapSrcColIdToVal map[string]string, toddl common.ToDdl) ([]string, []interface{}, error) {
	spCols, spVals, err := processTransformationFunction(conv, tableId, mapSrcColIdToVal, toddl)
	if err != nil {
		return nil, nil, fmt.Errorf("Error occured while processing data transformation")
	}
	// call method for synthetic primary key
	// call method for shard id column
	return spCols, spVals, err
}

func processTransformationFunction(conv *internal.Conv, tableId string, mapSrcColIdToVal map[string]string, toddl common.ToDdl) ([]string, []interface{}, error) {

	for _, rule := range conv.Transformations {
		if rule.AssociatedObjects == tableId {
			var x interface{}
			var err error
			switch rule.Function {
			case MathOperation:
				x, err = applyMathOpNew(rule, conv, tableId, mapSrcColIdToVal)
				fmt.Println(x)
			}
			if err != nil {
				return nil, nil, err
			}
		}
	}
	return nil, nil, nil
}

func applyMathOpNew(rule internal.Transformation, conv *internal.Conv, tableId string, mapSourceColIdToVal map[string]string, tempVar map[string]variable, toddl common.ToDdl) (interface{}, error) {
	firstInput, operator, secondInput, err := extract2Operand1Operator(rule, mapSourceColIdToVal, conv, tableId, tempVar, toddl, MathOperation)
	if err != nil {
		return nil, err
	}

	err = validateOutputDatatype(rule, conv, tableId, MathOperation)
	if err != nil {
		return nil, err
	}
	var output interface{}
	switch operator.value.(string) {
	case "add":
		switch firstInput.value.(type) {
		case int64:
			switch secondInput.value.(type) {
			case int64:
				output = firstInput.value.(int64) + secondInput.value.(int64)
			case float64:
				output = float64(firstInput.value.(int64)) + secondInput.value.(float64)
			default:
				return nil, fmt.Errorf("unsupported type for second input value: %T", secondInput.value)
			}
		case float64:
			switch secondInput.value.(type) {
			case int64:
				output = firstInput.value.(float64) + float64(secondInput.value.(int64))
			case float64:
				output = firstInput.value.(float64) + secondInput.value.(float64)
			default:
				return nil, fmt.Errorf("unsupported type for second input value: %T", secondInput.value)
			}
		default:
			return nil, fmt.Errorf("unsupported type for first input value: %T", firstInput.value)
		}
	case "subtract":
		switch firstInput.value.(type) {
		case int64:
			switch secondInput.value.(type) {
			case int64:
				output = firstInput.value.(int64) - secondInput.value.(int64)
			case float64:
				output = float64(firstInput.value.(int64)) - secondInput.value.(float64)
			default:
				return nil, fmt.Errorf("unsupported type for second input value: %T", secondInput.value)
			}
		case float64:
			switch secondInput.value.(type) {
			case int64:
				output = firstInput.value.(float64) - float64(secondInput.value.(int64))
			case float64:
				output = firstInput.value.(float64) - secondInput.value.(float64)
			default:
				return nil, fmt.Errorf("unsupported type for second input value: %T", secondInput.value)
			}
		default:
			return nil, fmt.Errorf("unsupported type for first input value: %T", firstInput.value)
		}
	case "multiply":
		switch firstInput.value.(type) {
		case int64:
			switch secondInput.value.(type) {
			case int64:
				output = firstInput.value.(int64) * secondInput.value.(int64)
			case float64:
				output = float64(firstInput.value.(int64)) * secondInput.value.(float64)
			default:
				return nil, fmt.Errorf("unsupported type for second input value: %T", secondInput.value)
			}
		case float64:
			switch secondInput.value.(type) {
			case int64:
				output = firstInput.value.(float64) * float64(secondInput.value.(int64))
			case float64:
				output = firstInput.value.(float64) * secondInput.value.(float64)
			default:
				return nil, fmt.Errorf("unsupported type for second input value: %T", secondInput.value)
			}
		default:
			return nil, fmt.Errorf("unsupported type for first input value: %T", firstInput.value)
		}
	case "divide":
		switch firstInput.value.(type) {
		case int64:
			switch secondInput.value.(type) {
			case int64:
				output = float64(firstInput.value.(int64)) / float64(secondInput.value.(int64))
			case float64:
				output = float64(firstInput.value.(int64)) / secondInput.value.(float64)
			default:
				return nil, fmt.Errorf("unsupported type for second input value: %T", secondInput.value)
			}
		case float64:
			switch secondInput.value.(type) {
			case int64:
				output = firstInput.value.(float64) / float64(secondInput.value.(int64))
			case float64:
				output = firstInput.value.(float64) / secondInput.value.(float64)
			default:
				return nil, fmt.Errorf("unsupported type for second input value: %T", secondInput.value)
			}
		default:
			return nil, fmt.Errorf("unsupported type for first input value: %T", firstInput.value)
		}
	case "mod":
		switch firstInput.value.(type) {
		case int64:
			switch secondInput.value.(type) {
			case int64:
				output = firstInput.value.(int64) % secondInput.value.(int64)
			case float64:
				output = math.Mod(float64(firstInput.value.(int64)), secondInput.value.(float64))
			default:
				return nil, fmt.Errorf("unsupported type for second input value: %T", secondInput)
			}
		case float64:
			switch secondInput.value.(type) {
			case int64:
				output = math.Mod(firstInput.value.(float64), float64(secondInput.value.(int64)))
			case float64:
				output = math.Mod(firstInput.value.(float64), secondInput.value.(float64))
			default:
				return nil, fmt.Errorf("unsupported type for second input value: %T", secondInput)
			}
		default:
			return nil, fmt.Errorf("unsupported type for first input value: %T", firstInput)
		}
	}

	return output, nil
}

func validateOutputDatatype(rule internal.Transformation, conv *internal.Conv, tableId, functionName string) error {
	actionConfig := rule.ActionConfig
	if rule.Action == WriteToColumnAction {
		column, ok := actionConfig["column"].(string)
		if !ok {
			return fmt.Errorf("could not parse column of action config with rule id:%s", rule.Id)
		}
		if !checkIfExists(conv.SpSchema[tableId].ColDefs[column].T.Name, SupportedFunctionsConst.Functions[functionName].Output) {
			return fmt.Errorf("Generated output doesn't match with column datatype for rule id: %s", rule.Id)
		}
	}
	return nil
}

func extract2Operand1Operator(rule internal.Transformation, mapSourceColIdToVal map[string]string, conv *internal.Conv, tableId string, tempVar map[string]variable, toddl common.ToDdl, functionName string) (inputValue, inputValue, inputValue, error) {
	firstInput, err := extractAndValidateInput(rule, 0, mapSourceColIdToVal, conv, tableId, tempVar, toddl, functionName, operand)
	secondInput, err := extractAndValidateInput(rule, 1, mapSourceColIdToVal, conv, tableId, tempVar, toddl, functionName, operator)
	thirdInput, err := extractAndValidateInput(rule, 2, mapSourceColIdToVal, conv, tableId, tempVar, toddl, functionName, operand)
	return firstInput, secondInput, thirdInput, err
}

func extractInput(input internal.Input, mapSourceColIdToVal map[string]string, conv *internal.Conv, tableId string, tempVar map[string]variable, toddl common.ToDdl) (inputValue, error) {
	switch input.Type {
	case SourceColumn:
		ty, _ := toddl.ToSpannerType(conv, "", conv.SrcSchema[tableId].ColDefs[input.Value].Type)
		parsedValue, err := convScalar(conv, ty.Name, mapSourceColIdToVal[input.Value])
		fmt.Println("source Value parsed:", input.Value)
		if err != nil {
			return inputValue{}, err
		}
		return inputValue{
			value:     parsedValue,
			inputType: input.Type,
			dataType:  ty.Name,
		}, nil
	case Operator:
		value := input.Value
		return inputValue{
			value:     value,
			inputType: input.Type,
			dataType:  "",
		}, nil
	case Static:
		dataType := input.DataType
		if conv.SpDialect == constants.DIALECT_POSTGRESQL {
			standardType, ok := ddl.PGSQL_TO_STANDARD_TYPE_TYPEMAP[dataType]
			if ok {
				dataType = standardType
			}
		}
		parsedValue, err := convScalar(conv, dataType, input.Value)
		if err != nil {
			return inputValue{}, err
		}
		return inputValue{
			value:     parsedValue,
			inputType: input.Type,
			dataType:  dataType,
		}, nil
	case Variable:
		varValue, ok := tempVar[input.Value]
		if !ok {
			return inputValue{}, fmt.Errorf("could not get variable value for: %s", input.Value)
		}
		return inputValue{
			value:     varValue.value,
			inputType: input.Type,
			dataType:  varValue.dataType,
		}, nil
	}
	return inputValue{}, fmt.Errorf("unsupported input type: %s", input.Type)
}

func extractAndValidateInput(rule internal.Transformation, index int64, mapSourceColIdToVal map[string]string, conv *internal.Conv, tableId string, tempVar map[string]variable, toddl common.ToDdl, functionName string, input input) (inputValue, error) {
	extractedInput, err := extractInput(rule.Input[index], mapSourceColIdToVal, conv, tableId, tempVar, toddl)
	if err != nil {
		return inputValue{}, err
	}
	var found bool
	if input == operator {
		found = checkIfExists(extractedInput.value.(string), SupportedFunctionsConst.Functions[functionName].Input[index])
	} else if input == operand {
		found = checkIfExists(extractedInput.dataType, SupportedFunctionsConst.Functions[functionName].Input[index])
	}
	if !found {
		return extractedInput, fmt.Errorf("Input type not valid for: %s", rule.Input[index])
	}
	return extractedInput, nil
}

func checkIfExists(val string, allowedVals []string) bool {
	found := false
	for _, allowedVal := range allowedVals {
		if allowedVal == val {
			found = true
			break
		}
	}
	return found
}

func ProcessDataTransformation(conv *internal.Conv, tableId string, cvtCols []string, cvtVals []interface{}, mapSrcColIdToVal map[string]string, toddl common.ToDdl) ([]string, []interface{}, error) {
	mapSpannerColIdToVal := make(map[string]interface{})
	for i, spCol := range cvtCols {
		mapSpannerColIdToVal[spCol] = cvtVals[i]
	}
	var (
		spannerVals []interface{}
		spannerCols []string
	)
	tempVar := make(map[string]variable)
	for _, rule := range conv.Transformations {
		if rule.AssociatedObjects == tableId {
			inputs := rule.Input
			var firstInput, secondInput, operator inputValue
			for _, input := range inputs {
				y, err := getValue(input, mapSrcColIdToVal, conv, tableId, tempVar, toddl)
				if err != nil {
					return nil, nil, fmt.Errorf("could not parse value for:%s, error:%w", input, err)
				}
				if y.inputType == Operator {
					operator = y
				} else {
					isEmpty := reflect.DeepEqual(firstInput, inputValue{})
					if isEmpty {
						firstInput = y
					} else {
						secondInput = y
					}
				}
			}
			var x interface{}
			var err error
			switch rule.Function {
			case MathOperation:
				x, err = applyMathOp(firstInput, secondInput, operator)
			case NoOp:
				x = firstInput.value
			case GenerateUUID:
				x = generateUuid()
			case BitReverse:
				x, err = bitReverse(firstInput)
			case Floor:
				x, err = applyFloor(firstInput)
			case Ceil:
				x, err = applyCeil(firstInput)
			case Compare:
				x, err = applyCompare(firstInput, secondInput, operator)
			case LogicalOp:
				x, err = applyLogicalOp(firstInput, secondInput, operator)
			}
			if err != nil {
				return nil, nil, err
			}
			actionConfig := rule.ActionConfig
			if rule.Action == WriteToColumnAction {
				column, ok := actionConfig["column"].(string)
				if !ok {
					return nil, nil, fmt.Errorf("could not parse column of action config with rule id:%s", rule.Id)
				}
				mapSpannerColIdToVal[column] = x
			} else if rule.Action == WriteToVariableAction {
				varValue, ok := actionConfig["VarName"].(map[string]interface{})
				if !ok {
					return nil, nil, fmt.Errorf("could not parse variable of action config with rule id:%s", rule.Id)
				}
				value, ok := varValue["Value"].(string)
				if !ok {
					return nil, nil, fmt.Errorf("could not parse value for variable: %s", varValue)
				}
				dataType, ok := varValue["Datatype"].(string)
				if !ok {
					return nil, nil, fmt.Errorf("could not parse datatype for variable: %s", varValue)
				}
				if conv.SpDialect == constants.DIALECT_POSTGRESQL {
					standardType, ok := ddl.PGSQL_TO_STANDARD_TYPE_TYPEMAP[dataType]
					if ok {
						dataType = standardType
					}
				}
				tempVar[value] = variable{
					value:    x,
					dataType: dataType,
				}
			} else if rule.Action == FilterAction {
				filterAction, ok := actionConfig[Include].(string)
				if !ok {
					return nil, nil, fmt.Errorf("could not parse filter action: %s", actionConfig[Include])
				}
				if x == true && filterAction == "true" {
					return nil, nil, nil
				}
			}
		}
	}

	keys := make([]string, 0, len(mapSpannerColIdToVal))
	for key := range mapSpannerColIdToVal {
		keys = append(keys, key)
	}

	// Sort the keys
	sort.Strings(keys)

	for _, key := range keys {
		spannerCols = append(spannerCols, conv.SpSchema[tableId].ColDefs[key].Name)
		spannerVals = append(spannerVals, mapSpannerColIdToVal[key])
	}
	if aux, ok := conv.SyntheticPKeys[tableId]; ok {
		spannerCols = append(spannerCols, conv.SpSchema[tableId].ColDefs[aux.ColId].Name)
		spannerVals = append(spannerVals, fmt.Sprintf("%d", int64(bits.Reverse64(uint64(aux.Sequence)))))
		aux.Sequence++
		conv.SyntheticPKeys[tableId] = aux
	}
	return spannerCols, spannerVals, nil
}

func getValue(input internal.Input, mapSourceColIdToVal map[string]string, conv *internal.Conv, tableId string, tempVar map[string]variable, toddl common.ToDdl) (inputValue, error) {
	switch input.Type {
	case SourceColumn:
		value := input.Value
		fmt.Println("Source Column:", mapSourceColIdToVal[value])
		ty, _ := toddl.ToSpannerType(conv, "", conv.SrcSchema[tableId].ColDefs[value].Type)
		parsedValue, err := convScalar(conv, ty.Name, mapSourceColIdToVal[value])
		fmt.Println("source Value parsed:", value)
		if err != nil {
			return inputValue{}, err
		}
		return inputValue{
			value:     parsedValue,
			inputType: input.Type,
			dataType:  ty.Name,
		}, nil
	case Operator:
		value := input.Value
		return inputValue{
			value:     value,
			inputType: input.Type,
			dataType:  "",
		}, nil
	case Static:
		dataType := input.DataType
		if conv.SpDialect == constants.DIALECT_POSTGRESQL {
			standardType, ok := ddl.PGSQL_TO_STANDARD_TYPE_TYPEMAP[dataType]
			if ok {
				dataType = standardType
			}
		}
		value := input.Value
		parsedValue, err := convScalar(conv, dataType, value)
		if err != nil {
			return inputValue{}, err
		}
		return inputValue{
			value:     parsedValue,
			inputType: input.Type,
			dataType:  dataType,
		}, nil
	case Variable:
		value := input.Value
		varValue, ok := tempVar[value]
		if !ok {
			return inputValue{}, fmt.Errorf("could not get variable value for: %s", value)
		}
		return inputValue{
			value:     varValue.value,
			inputType: input.Type,
			dataType:  varValue.dataType,
		}, nil
	}
	return inputValue{}, fmt.Errorf("unsupported input type: %s", input.Type)
}

func bitReverse(firstInput inputValue) (int64, error) {
	switch firstInput.value.(type) {
	case int64:
		return int64(bits.Reverse64(uint64(firstInput.value.(int64)))), nil
	default:
		return 0, fmt.Errorf("unsupported type for first input value: %T", firstInput.value)
	}
}

func generateUuid() string {
	uuid := uuid.New()
	return uuid.String()
}

func applyFloor(firstInput inputValue) (interface{}, error) {
	switch firstInput.value.(type) {
	case int64:
		return firstInput.value.(int64), nil
	case float64:
		return math.Floor(firstInput.value.(float64)), nil
	}
	return nil, fmt.Errorf("unsupported data type: %T", firstInput.value)
}

func applyCeil(firstInput inputValue) (interface{}, error) {
	switch firstInput.value.(type) {
	case int64:
		return firstInput.value.(int64), nil
	case float64:
		return math.Ceil(firstInput.value.(float64)), nil
	}
	return nil, fmt.Errorf("unsupported data type: %T", firstInput.value)
}

func applyLogicalOp(firstInput, secondInput, operator inputValue) (interface{}, error) {
	switch operator.value {
	case AndOperator:
		return logicalAnd(firstInput, secondInput)
	case OrOperator:
		return logicalOr(firstInput, secondInput)
	case XorOperator:
		return logicalXor(firstInput, secondInput)
	case NotOperator:
		return logicalNot(firstInput)
	}
	return nil, fmt.Errorf("unsupported comparison operation: %s", operator.value)
}

// Logical AND operator
func logicalAnd(a, b interface{}) (bool, error) {
	switch a := a.(type) {
	case bool:
		if b, ok := b.(bool); ok {
			return a && b, nil
		}
	case int64:
		if b, ok := b.(int64); ok {
			return a != 0 && b != 0, nil
		}
	case float64:
		if b, ok := b.(float64); ok {
			return a != 0.0 && b != 0.0, nil
		}
	case string:
		if b, ok := b.(string); ok {
			return a != "" && b != "", nil
		}
	case []byte:
		if b, ok := b.([]byte); ok {
			return len(a) != 0 && len(b) != 0, nil
		}
	case civil.Date:
		if b, ok := b.(civil.Date); ok {
			return a != civil.Date{} && b != civil.Date{}, nil
		}
	case time.Time:
		if b, ok := b.(time.Time); ok {
			return !a.IsZero() && !b.IsZero(), nil
		}
	}
	return false, fmt.Errorf("unsupported type for logical operation: %T", a)
}

// Logical OR operator
func logicalOr(a, b interface{}) (bool, error) {
	switch a := a.(type) {
	case bool:
		if b, ok := b.(bool); ok {
			return a || b, nil
		}
	case int64:
		if b, ok := b.(int64); ok {
			return a != 0 || b != 0, nil
		}
	case float64:
		if b, ok := b.(float64); ok {
			return a != 0.0 || b != 0.0, nil
		}
	case string:
		if b, ok := b.(string); ok {
			return a != "" || b != "", nil
		}
	case []byte:
		if b, ok := b.([]byte); ok {
			return len(a) != 0 || len(b) != 0, nil
		}
	case civil.Date:
		if b, ok := b.(civil.Date); ok {
			return a != civil.Date{} || b != civil.Date{}, nil
		}
	case time.Time:
		if b, ok := b.(time.Time); ok {
			return !a.IsZero() || !b.IsZero(), nil
		}
	}
	return false, fmt.Errorf("unsupported type for logical operation: %T", a)
}

// Logical XOR operator
func logicalXor(a, b interface{}) (bool, error) {
	switch a := a.(type) {
	case bool:
		if b, ok := b.(bool); ok {
			return (a && !b) || (!a && b), nil
		}
	case int64:
		if b, ok := b.(int64); ok {
			return (a != 0) != (b != 0), nil
		}
	case float64:
		if b, ok := b.(float64); ok {
			return (a != 0.0) != (b != 0.0), nil
		}
	case string:
		if b, ok := b.(string); ok {
			return (a != "") != (b != ""), nil
		}
	case []byte:
		if b, ok := b.([]byte); ok {
			return (len(a) != 0) != (len(b) != 0), nil
		}
	case civil.Date:
		if b, ok := b.(civil.Date); ok {
			return (a != civil.Date{}) != (b != civil.Date{}), nil
		}
	case time.Time:
		if b, ok := b.(time.Time); ok {
			return (!a.IsZero()) != (!b.IsZero()), nil
		}
	}
	return false, fmt.Errorf("unsupported type for logical operation: %T", a)
}

// Logical NOT operator
func logicalNot(a interface{}) (bool, error) {
	switch a := a.(type) {
	case bool:
		return !a, nil
	case int64:
		return a == 0, nil
	case float64:
		return a == 0.0, nil
	case string:
		return a == "", nil
	case []byte:
		return len(a) == 0, nil
	case civil.Date:
		return a == civil.Date{}, nil
	case time.Time:
		return a.IsZero(), nil
	}
	return false, fmt.Errorf("unsupported type for logical operation: %T", a)
}

func applyCompare(firstInput, secondInput, operator inputValue) (interface{}, error) {
	switch operator.value {
	case EqualToOperator:
		return compareEqual(firstInput, secondInput)
	case GreaterThanOperator:
		return compareGreaterThan(firstInput, secondInput)
	case LesserThanOperator:
		return compareLessThan(firstInput, secondInput)
	}
	return nil, fmt.Errorf("unsupported comparison operation: %s", operator.value)
}

func compareEqual(firstInput, secondInput inputValue) (bool, error) {
	if firstInput.dataType == secondInput.dataType || firstInput.dataType == ddl.Int64 {
		return firstInput.value.(int64) == secondInput.value.(int64), nil
	} else if firstInput.dataType == secondInput.dataType || firstInput.dataType == ddl.Float64 {
		return firstInput.value.(float64) == secondInput.value.(float64), nil
	} else if firstInput.dataType == secondInput.dataType || firstInput.dataType == ddl.Bool {
		return firstInput.value.(bool) == secondInput.value.(bool), nil
	} else if firstInput.dataType == secondInput.dataType || firstInput.dataType == ddl.Bytes {
		return string(firstInput.value.([]byte)) == string(secondInput.value.([]byte)), nil
	} else if firstInput.dataType == secondInput.dataType || firstInput.dataType == ddl.Date {
		return firstInput.value.(civil.Date) == secondInput.value.(civil.Date), nil
	} else if firstInput.dataType == secondInput.dataType || firstInput.dataType == ddl.Timestamp {
		return firstInput.value.(time.Time).Equal(secondInput.value.(time.Time)), nil
	}
	return false, fmt.Errorf("unsupported data types for comparison: %T, %T", firstInput.value, secondInput.value)
}

func compareGreaterThan(firstInput, secondInput inputValue) (bool, error) {
	if firstInput.dataType == secondInput.dataType || firstInput.dataType == ddl.Int64 {
		return firstInput.value.(int64) > secondInput.value.(int64), nil
	} else if firstInput.dataType == secondInput.dataType || firstInput.dataType == ddl.Float64 {
		return firstInput.value.(float64) > secondInput.value.(float64), nil
	} else if firstInput.dataType == secondInput.dataType || firstInput.dataType == ddl.Date {
		return firstInput.value.(civil.Date).After(secondInput.value.(civil.Date)), nil
	} else if firstInput.dataType == secondInput.dataType || firstInput.dataType == ddl.Timestamp {
		return firstInput.value.(time.Time).After(secondInput.value.(time.Time)), nil
	}
	return false, fmt.Errorf("unsupported data types for comparison: %T, %T", firstInput.value, secondInput.value)
}

func compareLessThan(firstInput, secondInput inputValue) (bool, error) {
	if firstInput.dataType == secondInput.dataType || firstInput.dataType == ddl.Int64 {
		return firstInput.value.(int64) < secondInput.value.(int64), nil
	} else if firstInput.dataType == secondInput.dataType || firstInput.dataType == ddl.Float64 {
		return firstInput.value.(float64) < secondInput.value.(float64), nil
	} else if firstInput.dataType == secondInput.dataType || firstInput.dataType == ddl.Date {
		return firstInput.value.(civil.Date).Before(secondInput.value.(civil.Date)), nil
	} else if firstInput.dataType == secondInput.dataType || firstInput.dataType == ddl.Timestamp {
		return firstInput.value.(time.Time).Before(secondInput.value.(time.Time)), nil
	}
	return false, fmt.Errorf("unsupported data types for comparison: %T, %T", firstInput.value, secondInput.value)
}

func applyMathOp(firstInput, secondInput, operator inputValue) (interface{}, error) {
	switch operator.value.(string) {
	case "add":
		switch firstInput.value.(type) {
		case int64:
			switch secondInput.value.(type) {
			case int64:
				return firstInput.value.(int64) + secondInput.value.(int64), nil
			case float64:
				return float64(firstInput.value.(int64)) + secondInput.value.(float64), nil
			default:
				return nil, fmt.Errorf("unsupported type for second input value: %T", secondInput.value)
			}
		case float64:
			switch secondInput.value.(type) {
			case int64:
				return firstInput.value.(float64) + float64(secondInput.value.(int64)), nil
			case float64:
				return firstInput.value.(float64) + secondInput.value.(float64), nil
			default:
				return nil, fmt.Errorf("unsupported type for second input value: %T", secondInput.value)
			}
		default:
			return nil, fmt.Errorf("unsupported type for first input value: %T", firstInput.value)
		}
	case "subtract":
		switch firstInput.value.(type) {
		case int64:
			switch secondInput.value.(type) {
			case int64:
				return firstInput.value.(int64) - secondInput.value.(int64), nil
			case float64:
				return float64(firstInput.value.(int64)) - secondInput.value.(float64), nil
			default:
				return nil, fmt.Errorf("unsupported type for second input value: %T", secondInput.value)
			}
		case float64:
			switch secondInput.value.(type) {
			case int64:
				return firstInput.value.(float64) - float64(secondInput.value.(int64)), nil
			case float64:
				return firstInput.value.(float64) - secondInput.value.(float64), nil
			default:
				return nil, fmt.Errorf("unsupported type for second input value: %T", secondInput.value)
			}
		default:
			return nil, fmt.Errorf("unsupported type for first input value: %T", firstInput.value)
		}
	case "multiply":
		switch firstInput.value.(type) {
		case int64:
			switch secondInput.value.(type) {
			case int64:
				return firstInput.value.(int64) * secondInput.value.(int64), nil
			case float64:
				return float64(firstInput.value.(int64)) * secondInput.value.(float64), nil
			default:
				return nil, fmt.Errorf("unsupported type for second input value: %T", secondInput.value)
			}
		case float64:
			switch secondInput.value.(type) {
			case int64:
				return firstInput.value.(float64) * float64(secondInput.value.(int64)), nil
			case float64:
				return firstInput.value.(float64) * secondInput.value.(float64), nil
			default:
				return nil, fmt.Errorf("unsupported type for second input value: %T", secondInput.value)
			}
		default:
			return nil, fmt.Errorf("unsupported type for first input value: %T", firstInput.value)
		}
	case "divide":
		switch firstInput.value.(type) {
		case int64:
			switch secondInput.value.(type) {
			case int64:
				return float64(firstInput.value.(int64)) / float64(secondInput.value.(int64)), nil
			case float64:
				return float64(firstInput.value.(int64)) / secondInput.value.(float64), nil
			default:
				return nil, fmt.Errorf("unsupported type for second input value: %T", secondInput.value)
			}
		case float64:
			switch secondInput.value.(type) {
			case int64:
				return firstInput.value.(float64) / float64(secondInput.value.(int64)), nil
			case float64:
				return firstInput.value.(float64) / secondInput.value.(float64), nil
			default:
				return nil, fmt.Errorf("unsupported type for second input value: %T", secondInput.value)
			}
		default:
			return nil, fmt.Errorf("unsupported type for first input value: %T", firstInput.value)
		}
	case "mod":
		switch firstInput.value.(type) {
		case int64:
			switch secondInput.value.(type) {
			case int64:
				return firstInput.value.(int64) % secondInput.value.(int64), nil
			case float64:
				return math.Mod(float64(firstInput.value.(int64)), secondInput.value.(float64)), nil
			default:
				return nil, fmt.Errorf("unsupported type for second input value: %T", secondInput)
			}
		case float64:
			switch secondInput.value.(type) {
			case int64:
				return math.Mod(firstInput.value.(float64), float64(secondInput.value.(int64))), nil
			case float64:
				return math.Mod(firstInput.value.(float64), secondInput.value.(float64)), nil
			default:
				return nil, fmt.Errorf("unsupported type for second input value: %T", secondInput)
			}
		default:
			return nil, fmt.Errorf("unsupported type for first input value: %T", firstInput)
		}
	}

	return nil, fmt.Errorf("unsupported operator: %v", operator.value)
}
