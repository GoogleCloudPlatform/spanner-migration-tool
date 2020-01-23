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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestProgress(t *testing.T) {
	var total int64 = 4321
	p := NewProgress(total, "Progress", false)
	assert.Equal(t, 0, p.pct)
	time.Sleep(500 * time.Millisecond)
	for _, i := range []int64{1000, 2000, 3000} {
		p.MaybeReport(i)
		assert.Equal(t, int((100*i)/total), p.pct)
		time.Sleep(500 * time.Millisecond)
	}
	pct := p.pct
	p.MaybeReport(2000)
	assert.Equal(t, pct, p.pct) // pct is monotonic.
	p.MaybeReport(5000)
	assert.Equal(t, 100, p.pct) // Never exceed 100%.
	p.Done()
	// Test corner case where total is 0.
	p = NewProgress(0, "Progress", false)
	assert.Equal(t, 100, p.pct)
}
