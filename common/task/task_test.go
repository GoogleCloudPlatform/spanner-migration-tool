// Copyright 2024 Google LLC
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

/*
Package utils contains common helper functions used across multiple other packages.
Utils should not import any Spanner migration tool packages.
*/
package task

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/rand"
)

func TestWorkerPool(t *testing.T) {
	input := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}

	f := func(i int, mutex *sync.Mutex) TaskResult[int] {
		sleepTime := time.Duration(rand.Intn(1000 * 1000))
		time.Sleep(sleepTime)
		res := TaskResult[int]{Result: i, Err: nil}
		return res
	}

	r := RunParallelTasksImpl[int, int]{}
	out, _ := r.RunParallelTasks(input, 5, f, false)
	assert.Equal(t, len(input), len(out), fmt.Sprintln("jobs not processed"))
}
