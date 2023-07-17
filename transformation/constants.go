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

import "github.com/cloudspannerecosystem/harbourbridge/spanner/ddl"

const (
	MathOperation         = "mathOp"
	NoOp                  = "noOp"
	GenerateUUID          = "generateUUID"
	BitReverse            = "bitReverse"
	ToTimestamp           = "toTimestamp"
	ToInteger             = "toInteger"
	ToFloat               = "toFloat"
	Round                 = "round"
	Floor                 = "floor"
	Ceil                  = "ceil"
	Compare               = "compare"
	BinaryLogicalOp       = "binaryLogicalOp"
	UnaryLogicalOp        = "unaryLogicalOp"
	WriteToColumnAction   = "writeToColumn"
	WriteToVariableAction = "writeToVar"
	FilterAction          = "filter"
	Include               = "include"
	SourceColumn          = "source-column"
	Operator              = "operator"
	Static                = "static"
	Variable              = "variable"
	AndOperator           = "&&"
	OrOperator            = "||"
	XorOperator           = "^"
	NotOperator           = "!"
	EqualToOperator       = "=="
	GreaterThanOperator   = ">"
	LesserThanOperator    = "<"
	AddOperator           = "+"
	SubtractOperator      = "-"
	MultiplyOperator      = "*"
	DivideOperator        = "/"
	ModOperator           = "%"
)

var SupportedFunctionsConst = SupportedFunctions{
	Functions: map[string]FunctionDefinition{
		MathOperation: {
			Name:        MathOperation,
			Description: "Apply math operations: Add, subtract, multiply, divide, mod",
			Comment:     "",
			Input: [][]string{
				{ddl.Int64, ddl.Float64},
				{AddOperator, SubtractOperator, MultiplyOperator, DivideOperator, ModOperator},
				{ddl.Int64, ddl.Float64},
			},
			Output: []string{ddl.Int64, ddl.Float64},
		},
		GenerateUUID: {
			Name:        GenerateUUID,
			Description: "Generate a UUID",
			Comment:     "",
			Input:       [][]string{},
			Output:      []string{ddl.String},
		},
		NoOp: {
			Name:        NoOp,
			Description: "Return input as output",
			Comment:     "",
			Input: [][]string{
				{ddl.Int64, ddl.Float64, ddl.Bool, ddl.Date, ddl.JSON, ddl.Numeric, ddl.Date, ddl.Timestamp},
			},
			Output: []string{ddl.Int64, ddl.Float64, ddl.Bool, ddl.Date, ddl.JSON, ddl.Numeric, ddl.Date, ddl.Timestamp},
		},
		BitReverse: {
			Name:        BitReverse,
			Description: "Perform bit reversal on number",
			Comment:     "",
			Input: [][]string{
				{ddl.Int64},
			},
			Output: []string{ddl.Int64},
		},
		"toTimestamp": {
			Name:        "toTimestamp",
			Description: "Convert to timestamp",
			Comment:     "",
			Input: [][]string{
				{ddl.String},
				{ddl.String},
			},
			Output: []string{ddl.Timestamp},
		},
		ToInteger: {
			Name:        ToInteger,
			Description: "Convert to integer",
			Comment:     "",
			Input: [][]string{
				{ddl.String},
			},
			Output: []string{ddl.Int64},
		},
		ToFloat: {
			Name:        ToFloat,
			Description: "Convert to float",
			Comment:     "",
			Input: [][]string{
				{ddl.String},
			},
			Output: []string{ddl.Float64},
		},
		Floor: {
			Name:        Floor,
			Description: "Floor to the closest integer",
			Comment:     "",
			Input: [][]string{
				{ddl.Float64},
			},
			Output: []string{ddl.Int64},
		},
		Ceil: {
			Name:        Ceil,
			Description: "Raise to the closest ceiling",
			Comment:     "",
			Input: [][]string{
				{ddl.Float64},
			},
			Output: []string{ddl.Int64},
		},
		"round": {
			Name:        "round",
			Description: "Round to number of decimal places provided",
			Comment:     "",
			Input: [][]string{
				{ddl.Float64},
				{ddl.Int64},
			},
			Output: []string{ddl.Float64},
		},
		Compare: {
			Name:        Compare,
			Description: "perform comparison and return true/false",
			Comment:     "",
			Input: [][]string{
				{ddl.Int64, ddl.Float64, ddl.Bool, ddl.Date, ddl.JSON, ddl.Numeric, ddl.Date, ddl.Timestamp},
				{EqualToOperator, GreaterThanOperator, LesserThanOperator},
				{ddl.Int64, ddl.Float64, ddl.Bool, ddl.Date, ddl.JSON, ddl.Numeric, ddl.Date, ddl.Timestamp},
			},
			Output: []string{ddl.Bool},
		},
		BinaryLogicalOp: {
			Name:        BinaryLogicalOp,
			Description: "Perform binary logical operations - and / or / xor",
			Comment:     "",
			Input: [][]string{
				{ddl.Int64, ddl.Float64, ddl.Bool, ddl.Date, ddl.JSON, ddl.Numeric, ddl.Date, ddl.Timestamp},
				{AndOperator, OrOperator, XorOperator},
				{ddl.Int64, ddl.Float64, ddl.Bool, ddl.Date, ddl.JSON, ddl.Numeric, ddl.Date, ddl.Timestamp},
			},
			Output: []string{ddl.Bool},
		},
		UnaryLogicalOp: {
			Name:        UnaryLogicalOp,
			Description: "Perform unary logical operations - not",
			Comment:     "",
			Input: [][]string{
				{NotOperator},
				{ddl.Int64, ddl.Float64, ddl.Bool, ddl.Date, ddl.JSON, ddl.Numeric, ddl.Date, ddl.Timestamp},
			},
			Output: []string{ddl.Bool},
		},
		// Add more function definitions as needed
	},
}

var OperatorList = []string{AddOperator, SubtractOperator, MultiplyOperator, DivideOperator, ModOperator, AndOperator, OrOperator, XorOperator, NotOperator, EqualToOperator, LesserThanOperator, GreaterThanOperator}
