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
// WaitGroup is used to coordinate the goroutines that are executing the input that is passed.
// This is how we handle WaitGroup correctly - 
// 1. Initialize waitGroup
// 2. Defer execution to wg.Wait(). This ensures that all tasks added to the waitGroup will be processed.
// 3. Add len(input) tasks to the thread. This ensures that the waitGroup will track the completion of the tasks.
// 4. We then begin the execution by executing the processAync in goroutines (equal to the numWorkers)
// 5. When the processAsync processes a task, there are two possible outcomes - it will either pass or fail.
// 6. No matter the result, once the processing is complete, the task is acknowledged to be completed using wg.Done()
// 7a. [If no fastExit] No matter if it passes or fails, the results are added to the TaskResult[0] array.
// 7b. [If fastExit] Our goal here is to stop the worker pool prematurely and exit the processing. For this to happen, we need to prematurely ack the remaining tasks
//     that are not yet done, so that wg.Wait() does not keep on definitely waiting for all the tasks to complete. Hence, we cycle through the inputChannel where we sent the
//     work to be done and force ack all the tasks. This ensures that the waitGroup can how gracefully exit.
func (rpt *RunParallelTasksImpl[I, O]) RunParallelTasks(input []I, numWorkers int, f func(i I, mutex *sync.Mutex) TaskResult[O],
	fastExit bool) ([]TaskResult[O], error) {
	inputChannel := make(chan I, len(input))
	outputChannel := make(chan TaskResult[O], len(input))

	wg := &sync.WaitGroup{}
	defer wg.Wait()
	wg.Add(len(input))
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
				logger.Log.Debug("ignoring task to fast exit, calling wg.Done")
				wg.Done()
			}
			return out, res.Err
		}
		out = append(out, res)
	}
	logger.Log.Debug(fmt.Sprintf("completed processing of %d tasks", len(out)))
	return out, nil
}

func processAsync[I any, O any](f func(i I, mutex *sync.Mutex) TaskResult[O], in chan I,
	out chan TaskResult[O], mutex *sync.Mutex, wg *sync.WaitGroup) {
	for i := range in {
		logger.Log.Debug(fmt.Sprint("processing task for input", i))
		out <- f(i, mutex)
		wg.Done()
	}
}
