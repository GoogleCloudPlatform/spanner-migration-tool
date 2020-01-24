// Copyright 2019 Google LLC
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

package internal

import (
	"fmt"
)

// TODO: Replace verbose with one of the existing go logging
// libraries that provides support for different log-levels.
// Our main goal is to support '-v' (verbose mode) where we log
// lots of low-level details of conversion for debugging.
// Likely candidate: go.uber.org/zap.

var chatty = false

// Verbose returns true if verbose mode is enabled.
func Verbose() bool {
	return chatty
}

// VerboseInit determines whether verbose mode is enabled.
// Generally there should be one call to VerboseInit at startup.
func VerboseInit(b bool) {
	chatty = b
}

// VerbosePrintf prints to stdout if verbose is enabled.
func VerbosePrintf(format string, a ...interface{}) {
	if Verbose() {
		fmt.Printf(format, a...)
	}
}

// VerbosePrintln prints to stdout if verbose is enabled.
func VerbosePrintln(a ...interface{}) {
	if Verbose() {
		fmt.Println(a...)
	}
}
