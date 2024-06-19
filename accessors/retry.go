// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package retry

import (
	"errors"
	"reflect"
	"time"
)


const (
	DeadlineExceeded = "DEADLINE_EXCEEDED"
)

func Retry(
	fn interface{},
	retryCount int,
	retryableErrors []string,
) ([]reflect.Value, error) {
	if retryCount <= 0 {
		retryCount = 5
	}
	if retryableErrors == nil {
		retryableErrors = []string{DeadlineExceeded}
	}

	fnValue := reflect.ValueOf(fn)
	fnType := fnValue.Type()
	if fnType.Kind() != reflect.Func {
		return nil, errors.New("provided argument is not a function")
	}

	delay := time.Second

	for i := 0; i < retryCount; i++ {
		results := fnValue.Call(nil)
		if len(results) == 0 {
			return results, nil
		}

		// Check for an error as the last return value
		err, ok := results[len(results)-1].Interface().(error)
		if !ok || err == nil {
			return results, nil
		}

		// Check if the error is retryable
		retry := false
		for _, code := range retryableErrors {
			if errors.Is(err, errors.New(code)) {
				retry = true
				break
			}
		}

		if !retry {
			return results, err
		}

		// Wait before retrying
		time.Sleep(delay)
		delay *= 2
	}

	return nil, errors.New("max retries exceeded")
}