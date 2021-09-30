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
	"strings"
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
}

// NewProgress creates and returns a Progress instance.
func NewProgress(total int64, message string, verbose, fractional bool) *Progress {
	p := &Progress{total, 0, 0, message, verbose, fractional}
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
		if p.fractional {
			p.reportFraction(false)
			return
		}
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
	if firstCall {
		fmt.Printf("%s: %d/%d", p.message, p.progress, p.total)
	} else {
		delStr := strings.Repeat("\b", countDigits(p.progress-1)+countDigits(p.total)+1)
		fmt.Printf(delStr+"%d/%d", p.progress, p.total)
	}
	if p.progress == p.total {
		fmt.Printf("\n")
	}
}

func countDigits(n int64) int {
	if n == 0 {
		return 1
	}
	res := 0
	for n > 0 {
		n /= 10
		res += 1
	}
	return res
}
