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

package cassandra

import (
	"github.com/gocql/gocql"
)

// SessionInterface allows for mocking gocql.Session.
type SessionInterface interface {
    Query(stmt string, values ...interface{}) QueryInterface
    Close()
}

// QueryInterface allows for mocking gocql.Query.
type QueryInterface interface {
	Iter() IterInterface
}

// IterInterface allows for mocking gocql.Iter.
type IterInterface interface {
	Scan(dest ...interface{}) bool
	Close() error
}

// sessionWrapper implements SessionInterface using *gocql.Session.
type sessionWrapper struct {
	session *gocql.Session
}

func (s *sessionWrapper) Query(stmt string, values ...interface{}) QueryInterface {
	return &queryWrapper{query: s.session.Query(stmt, values...)}
}

func (s *sessionWrapper) Close() {
	s.session.Close()
}

// queryWrapper implements QueryInterface.
type queryWrapper struct {
	query *gocql.Query
}

func (q *queryWrapper) Iter() IterInterface {
	return &iterWrapper{iter: q.query.Iter()}
}

// iterWrapper implements IterInterface.
type iterWrapper struct {
	iter *gocql.Iter
}

func (i *iterWrapper) Scan(dest ...interface{}) bool {
	return i.iter.Scan(dest...)
}

func (i *iterWrapper) Close() error {
	return i.iter.Close()
}

func NewSessionWrapper(s *gocql.Session) SessionInterface {
    return &sessionWrapper{session: s}
}