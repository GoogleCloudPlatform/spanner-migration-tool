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

const (
	MathOperation         = "mathOp"
	NoOp                  = "noOp"
	GenerateUUID          = "generateUUID"
	BitReverse            = "bitReverse"
	Floor                 = "floor"
	Ceil                  = "ceil"
	Compare               = "compare"
	LogicalOp             = "logicalOp"
	WriteToColumnAction   = "writeToColumn"
	WriteToVariableAction = "writeToVar"
	FilterAction          = "filter"
	Include               = "include"
	SourceColumn          = "source-column"
	Operator              = "operator"
	Static                = "static"
	Variable              = "variable"
	AndOperator           = "and"
	OrOperator            = "or"
	XorOperator           = "xor"
	NotOperator           = "not"
	EqualToOperator       = "equalTo"
	GreaterThanOperator   = "greaterThan"
	LesserThanOperator    = "lessThan"
	AddOperator           = "add"
	SubtractOperator      = "subtract"
	MultiplyOperator      = "multiply"
	DivideOperator        = "divide"
	ModOperator           = "mod"
)
