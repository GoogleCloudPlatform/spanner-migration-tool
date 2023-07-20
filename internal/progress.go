// Copyright 2020 Google LLC
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

// Package internal implements database-agnostic functionality for
// HarbourBridge.
package internal

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"go.uber.org/zap"
)

// Progress provides console progress functionality. i.e. it reports what
// percentage of a task is complete to the console, overwriting previous
// progress percentage with new progress.
type Progress struct {
	total      int64  // How much we have to do.
	progress   int64  // How much we have done so far.
	pct        int    // Percentage done i.e. progress/total * 100
	message    string // Name of task being monitored.
	verbose    bool   // If true, print detailed info about each progress step.
	fractional bool   // If true, report progress in fractions instead of percentages.
	ProgressStatus
}

// ProgressStatus specifies a stage of migration.
type ProgressStatus int

// Defines the progress statuses that we track
const (
	DefaultStatus ProgressStatus = iota
	SchemaMigrationComplete
	SchemaCreationInProgress
	DataMigrationComplete
	DataWriteInProgress
	ForeignKeyUpdateInProgress
	ForeignKeyUpdateComplete
)

// NewProgress creates and returns a Progress instance.
func NewProgress(total int64, message string, verbose, fractional bool, progressStatus int) *Progress {
	p := &Progress{total, 0, 0, message, verbose, fractional, ProgressStatus(progressStatus)}
	if total == 0 {
		p.pct = 100
	}
	if p.fractional {
		p.reportFraction(true)
	} else {
		p.reportPct(true)
	}
	return p
}

// MaybeReport updates the state of p with the new 'progress' measure.
// If this update changes pct (integer part of percentage-done),
// MaybeReport will print out the new percentage, overwriting the previous
// percentage.
func (p *Progress) MaybeReport(progress int64) {
	if progress > p.progress {
		p.progress = progress
		var pct int
		if p.total > 0 {
			pct = int((progress * 100) / p.total)
		} else {
			pct = 100
		}
		if pct > 100 {
			pct = 100
		}
		if pct > p.pct {
			p.pct = pct
		}
		if p.fractional {
			p.reportFraction(false)
		} else {
			p.reportPct(false)
		}

	}
}

// Done signals completion, and will report 100% if it hasn't already
// been reported.
func (p *Progress) Done() {
	p.MaybeReport(p.total)
}

func (p *Progress) reportPct(firstCall bool) {
	if p.verbose {
		fmt.Printf("%s: %2d%%\n", p.message, p.pct)
		return
	}
	logger.Log.Debug(p.message, zap.Int("Progress", p.pct))
	if firstCall {
		fmt.Printf("%s: %2d%%", p.message, p.pct)
	} else {
		fmt.Printf("\b\b\b%2d%%", p.pct)
	}
	if p.pct == 100 {
		fmt.Printf("\n")
	}
}

func (p *Progress) reportFraction(firstCall bool) {
	if p.verbose {
		fmt.Printf("%s: %d/%d\n", p.message, p.progress, p.total)
		return
	}
	logger.Log.Debug(p.message, zap.Float32("Progress", float32(p.progress/p.total)))
	if firstCall {
		fmt.Printf("%s: %d/%d", p.message, p.progress, p.total)
	} else {
		// We delete the previous outputted fraction hence, adding backspace that many number of times.
		// The length of the previous outputted fraction would be len(p.progress-1) +len(total) and 1 for the "/".
		delStr := strings.Repeat("\b", len(strconv.Itoa(int(p.progress-1)))+1+len(strconv.Itoa(int(p.total))))
		fmt.Printf(delStr+"%d/%d", p.progress, p.total)
	}
	if p.progress == p.total {
		fmt.Printf("\n")
	}
}

func (p *Progress) ReportProgress() (int, int) {
	return int(p.pct), int(p.ProgressStatus)
}

func (p *Progress) UpdateProgress(message string, pct int, progressStatus ProgressStatus) {
	p.message = message
	p.pct = pct
	p.ProgressStatus = progressStatus
}
