package spannerclient

import (
	"context"

	"cloud.google.com/go/spanner"
)

type SpannerClientMock struct {
	SingleMock func() ReadOnlyTransaction
}

type ReadOnlyTransactionMock struct {
	QueryMock func(ctx context.Context, stmt spanner.Statement) RowIterator
}

type RowIteratorMock struct {
	NextMock func() (*spanner.Row, error)
	StopMock func()
}

func (scm SpannerClientMock) Single() ReadOnlyTransaction {
	return scm.SingleMock()
}

func (rom ReadOnlyTransactionMock) Query(ctx context.Context, stmt spanner.Statement) RowIterator {
	return rom.QueryMock(ctx, stmt)
}

func (rim RowIteratorMock) Next() (*spanner.Row, error) {
	return rim.NextMock()
}

func (rim RowIteratorMock) Stop() {
	rim.StopMock()
}