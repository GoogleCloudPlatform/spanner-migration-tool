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

	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
)

type RunParallelTasksInterface[I any, O any] interface {
	RunParallelTasks(input []I, numWorkers int, f func(i I, mutex *sync.Mutex) TaskResult[O], fastExit bool) ([]TaskResult[O], error)
}

type RunParallelTasksImpl[I any, O any] struct{}

type TaskResult[O any] struct {
	Result O
	Err    error
}

// Run multiple tasks in parallel. The tasks are expected to be thread safe
// input: 		List of inputs
// numWorkers: 	Size of worker pool
// f:			Function to execute the task
// fastExit: 	If an error is encountered, gracefully exit all running or queued tasks
// Returns an array of TaskResults and last error
func (rpt *RunParallelTasksImpl[I, O]) RunParallelTasks(input []I, numWorkers int, f func(i I, mutex *sync.Mutex) TaskResult[O],
	fastExit bool) ([]TaskResult[O], error) {
	inputChannel := make(chan I, len(input))
	outputChannel := make(chan TaskResult[O], len(input))

	wg := &sync.WaitGroup{}
	defer func() {
		for range inputChannel {
			logger.Log.Debug(fmt.Sprint("clearing out pending tasks"))
		}
		wg.Wait()
	}()

	mutex := &sync.Mutex{}
	logger.Log.Debug(fmt.Sprint("Number of configured workers are ", numWorkers))
	for w := 0; w < numWorkers; w++ {
		go processAsync(f, inputChannel, outputChannel, mutex, wg)
	}

	for _, in := range input {
		inputChannel <- in
	}
	close(inputChannel)

	out := []TaskResult[O]{}
	for i := 0; i < len(input); i++ {
		res := <-outputChannel
		if fastExit && res.Err != nil {
			logger.Log.Debug(fmt.Sprint("stopping worker pool due to encountered error", res.Err))
			for range inputChannel {
				logger.Log.Debug(fmt.Sprint("ignoring task to fast exit"))
			}
			wg.Wait()
			return out, res.Err
		}
		out = append(out, res)
	}
	logger.Log.Debug(fmt.Sprintf("completed processing of %d tasks", len(out)))
	return out, nil
}

func processAsync[I any, O any](f func(i I, mutex *sync.Mutex) TaskResult[O], in chan I,
	out chan TaskResult[O], mutex *sync.Mutex, wg *sync.WaitGroup) {
	wg.Add(1)
	for i := range in {
		logger.Log.Debug(fmt.Sprint("processing task for input", i))
		out <- f(i, mutex)
	}
	wg.Done()
}
