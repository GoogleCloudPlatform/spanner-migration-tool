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
			inputs, ok := rule.Input.([]interface{})
			if !ok {
				return nil, nil, fmt.Errorf("input not of expected type, input:%s", rule.Input)
			}
			var firstInput, secondInput, operator inputValue
			for _, input := range inputs {
				y, err := getValue(input, mapSrcColIdToVal, conv, tableId, tempVar, toddl)
				if err != nil {
					return nil, nil, fmt.Errorf("could not parse value for:%s, error:%w", input, err)
				}
				if y.inputType == "operator" {
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
			case "mathOp":
				x, err = applyMathOp(firstInput, secondInput, operator)
			case "noOp":
				x = firstInput.value
			case "generateUUID":
				x = generateUuid()
			case "bitReverse":
				x, err = bitReverse(firstInput)
			case "floor":
				x, err = applyFloor(firstInput)
			case "ceil":
				x, err = applyCeil(firstInput)
			case "compare":
				x, err = applyCompare(firstInput, secondInput, operator)
			}
			if err != nil {
				return nil, nil, err
			}
			actionConfig, ok := rule.ActionConfig.(map[string]interface{})
			if !ok {
				return nil, nil, fmt.Errorf("action config not of correct type for rule id:%s", rule.Id)
			}
			if rule.Action == "writeToColumn" {
				column, ok := actionConfig["column"].(string)
				if !ok {
					return nil, nil, fmt.Errorf("could not parse column of action config with rule id:%s", rule.Id)
				}
				mapSpannerColIdToVal[column] = x
			} else if rule.Action == "writeToVar" {
				varValue, ok := actionConfig["varName"].(map[string]interface{})
				if !ok {
					return nil, nil, fmt.Errorf("could not parse variable of action config with rule id:%s", rule.Id)
				}
				value, ok := varValue["value"].(string)
				if !ok {
					return nil, nil, fmt.Errorf("could not parse value for variable: %s", varValue)
				}
				dataType, ok := varValue["datatype"].(string)
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
			} else if rule.Action == "filter" {
				filterAction, ok := actionConfig["include"].(string)
				if !ok {
					return nil, nil, fmt.Errorf("could not parse filter action: %s", actionConfig["include"])
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

func getValue(inputInterface interface{}, mapSourceColIdToVal map[string]string, conv *internal.Conv, tableId string, tempVar map[string]variable, toddl common.ToDdl) (inputValue, error) {
	input := inputInterface.(map[string]interface{})
	fmt.Println(input)
	inputType, ok := input["type"].(string)
	if !ok {
		return inputValue{}, fmt.Errorf("could not parse type for input: %s", input["type"])
	}
	switch inputType {
	case "source-column":
		value, ok := input["value"].(string)
		if !ok {
			return inputValue{}, fmt.Errorf("could not parse value for input: %s", input)
		}
		fmt.Println("Source Column:", mapSourceColIdToVal[value])
		ty, _ := toddl.ToSpannerType(conv, "", conv.SrcSchema[tableId].ColDefs[value].Type)
		parsedValue, err := convScalar(conv, ty.Name, mapSourceColIdToVal[value])
		fmt.Println("source Value parsed:", value)
		if err != nil {
			return inputValue{}, err
		}
		return inputValue{
			value:     parsedValue,
			inputType: inputType,
			dataType:  ty.Name,
		}, nil
	case "operator":
		value, ok := input["value"].(string)
		if !ok {
			return inputValue{}, fmt.Errorf("could not parse value for input: %s", input)
		}
		return inputValue{
			value:     value,
			inputType: inputType,
			dataType:  "",
		}, nil
	case "static":
		dataType, ok := input["datatype"].(string)
		if !ok {
			return inputValue{}, fmt.Errorf("could not parse datatype for input: %s", input)
		}
		if conv.SpDialect == constants.DIALECT_POSTGRESQL {
			standardType, ok := ddl.PGSQL_TO_STANDARD_TYPE_TYPEMAP[dataType]
			if ok {
				dataType = standardType
			}
		}
		value, ok := input["value"].(string)
		if !ok {
			return inputValue{}, fmt.Errorf("could not parse value for input: %s", input)
		}
		parsedValue, err := convScalar(conv, dataType, value)
		if err != nil {
			return inputValue{}, err
		}
		return inputValue{
			value:     parsedValue,
			inputType: inputType,
			dataType:  dataType,
		}, nil
	case "variable":
		value, ok := input["value"].(string)
		if !ok {
			return inputValue{}, fmt.Errorf("could not parse value for input: %s", input)
		}
		varValue, ok := tempVar[value]
		if !ok {
			return inputValue{}, fmt.Errorf("could not get variable value for: %s", value)
		}
		return inputValue{
			value:     varValue.value,
			inputType: inputType,
			dataType:  varValue.dataType,
		}, nil
	}
	return inputValue{}, fmt.Errorf("unsupported input type: %s", inputType)
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

func applyCompare(firstInput, secondInput, operator inputValue) (interface{}, error) {
	switch operator.value {
	case "equalTo":
		return compareEqual(firstInput, secondInput)
	case "greaterThan":
		return compareGreaterThan(firstInput, secondInput)
	case "lessThan":
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
	fmt.Println(firstInput, secondInput)
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
