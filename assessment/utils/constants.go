// Copyright 2025 Google LLC
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

// Package constants contains constants used across multiple other packages.
// All string constants have a lower_case value and thus string matching is
// performend against other lower_case strings.
package utils

const (
	PARALLEL_TASK_RUNNER_COUNT int    = 40
	GEMINI_PRO_MODEL           string = "gemini-2.5-pro"
	GEMINI_FLASH_MODEL         string = "gemini-2.0-flash-001"
)

// SupportedFunctions is a map of supported Spanner GoogleSQL functions.
var SupportedFunctions = map[string]bool{
	// Aggregate Functions
	"ANY_VALUE":   true,
	"ARRAY_AGG":   true,
	"AVG":         true,
	"BIT_AND":     true,
	"BIT_OR":      true,
	"BIT_XOR":     true,
	"COUNT":       true,
	"COUNTIF":     true,
	"LOGICAL_AND": true,
	"LOGICAL_OR":  true,
	"MAX":         true,
	"MIN":         true,
	"STRING_AGG":  true,
	"SUM":         true,

	// Array Functions
	"ARRAY":                    true,
	"ARRAY_CONCAT":             true,
	"ARRAY_LENGTH":             true,
	"ARRAY_TO_STRING":          true,
	"GENERATE_ARRAY":           true,
	"GENERATE_DATE_ARRAY":      true,
	"GENERATE_TIMESTAMP_ARRAY": true,

	// Date and Time Functions
	"CURRENT_DATE":        true,
	"CURRENT_TIMESTAMP":   true,
	"DATE":                true,
	"DATE_ADD":            true,
	"DATE_SUB":            true,
	"DATE_DIFF":           true,
	"DATE_TRUNC":          true,
	"DATE_FROM_UNIX_DATE": true,
	"EXTRACT":             true,
	"FORMAT_DATE":         true,
	"FORMAT_TIMESTAMP":    true,
	"PARSE_DATE":          true,
	"PARSE_TIMESTAMP":     true,
	"TIMESTAMP":           true,
	"TIMESTAMP_ADD":       true,
	"TIMESTAMP_SUB":       true,
	"TIMESTAMP_DIFF":      true,
	"TIMESTAMP_TRUNC":     true,
	"TIMESTAMP_SECONDS":   true,
	"TIMESTAMP_MILLIS":    true,
	"TIMESTAMP_MICROS":    true,
	"UNIX_DATE":           true,

	// String Functions
	"BYTE_LENGTH":                  true,
	"CHAR_LENGTH":                  true,
	"CHARACTER_LENGTH":             true,
	"CODE_POINTS_TO_BYTES":         true,
	"CODE_POINTS_TO_STRING":        true,
	"CONCAT":                       true,
	"ENDS_WITH":                    true,
	"FORMAT":                       true,
	"FROM_BASE64":                  true,
	"FROM_HEX":                     true,
	"LENGTH":                       true,
	"LPAD":                         true,
	"LOWER":                        true,
	"LTRIM":                        true,
	"NORMALIZE":                    true,
	"NORMALIZE_AND_CASEFOLD":       true,
	"REGEXP_CONTAINS":              true,
	"REGEXP_EXTRACT":               true,
	"REGEXP_EXTRACT_ALL":           true,
	"REGEXP_INSTR":                 true,
	"REGEXP_REPLACE":               true,
	"REGEXP_SUBSTR":                true,
	"REPLACE":                      true,
	"REPEAT":                       true,
	"REVERSE":                      true,
	"RPAD":                         true,
	"RTRIM":                        true,
	"SAFE_CONVERT_BYTES_TO_STRING": true,
	"SPLIT":                        true,
	"STARTS_WITH":                  true,
	"STRPOS":                       true,
	"SUBSTR":                       true,
	"TO_BASE64":                    true,
	"TO_CODE_POINTS":               true,
	"TO_HEX":                       true,
	"TRIM":                         true,
	"UPPER":                        true,

	// JSON Functions
	"JSON_QUERY":     true,
	"JSON_VALUE":     true,
	"TO_JSON_STRING": true,

	// Mathematical Functions
	"ABS":           true,
	"SIGN":          true,
	"IS_INF":        true,
	"IS_NAN":        true,
	"IEEE_DIVIDE":   true,
	"RAND":          true,
	"SQRT":          true,
	"POW":           true,
	"POWER":         true,
	"EXP":           true,
	"LN":            true,
	"LOG":           true,
	"LOG10":         true,
	"GREATEST":      true,
	"LEAST":         true,
	"DIV":           true,
	"SAFE_DIVIDE":   true,
	"SAFE_MULTIPLY": true,
	"SAFE_ADD":      true,
	"SAFE_SUBTRACT": true,
	"MOD":           true,
	"ROUND":         true,
	"TRUNC":         true,
	"CEIL":          true,
	"CEILING":       true,
	"FLOOR":         true,
	"COS":           true,
	"COSH":          true,
	"ACOS":          true,
	"ACOSH":         true,
	"SIN":           true,
	"SINH":          true,
	"ASIN":          true,
	"ASINH":         true,
	"TAN":           true,
	"TANH":          true,
	"ATAN":          true,
	"ATANH":         true,
	"ATAN2":         true,

	// Hash Functions
	"FARM_FINGERPRINT": true,
	"MD5":              true,
	"SHA1":             true,
	"SHA256":           true,
	"SHA512":           true,

	// Conditional Functions
	"CASE":     true,
	"COALESCE": true,
	"IF":       true,
	"IFNULL":   true,
	"NULLIF":   true,
}

// SupportedOperators is a map of supported Spanner GoogleSQL operators.
var SupportedOperators = map[string]bool{
	// Arithmetic Operators
	"+": true,
	"-": true,
	"*": true,
	"/": true,

	// Bitwise Operators
	"&":  true,
	"|":  true,
	"^":  true,
	"~":  true,
	"<<": true,
	">>": true,

	// Comparison Operators
	"=":            true,
	"!=":           true,
	"<>":           true,
	"<":            true,
	">":            true,
	"<=":           true,
	">=":           true,
	"IS NULL":      true,
	"IS NOT NULL":  true,
	"IS TRUE":      true,
	"IS NOT TRUE":  true,
	"IS FALSE":     true,
	"IS NOT FALSE": true,
	"LIKE":         true,
	"NOT LIKE":     true,
	"BETWEEN":      true,
	"NOT BETWEEN":  true,
	"IN":           true,
	"NOT IN":       true,

	// Logical Operators
	"AND": true,
	"OR":  true,
	"NOT": true,

	// Concatenation Operator
	"||": true,
}
