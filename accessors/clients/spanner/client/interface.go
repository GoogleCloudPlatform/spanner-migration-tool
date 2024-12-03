package spannerclient

import (
	"context"

	"cloud.google.com/go/spanner"
)


type SpannerClient interface {
	Single() ReadOnlyTransaction
	DatabaseName() string
	Refresh(ctx context.Context, dbURI string) error
}

type ReadOnlyTransaction interface {
	Query(ctx context.Context, stmt spanner.Statement) RowIterator
}

type RowIterator interface {
	Next() (*spanner.Row, error)
	Stop()
}


// This implements the SpannerClient interface. This is the primary implementation that should be used in all places other than tests.
type SpannerClientImpl struct {
	spannerClient *spanner.Client
}

func NewSpannerClientImpl(ctx context.Context, dbURI string) (*SpannerClientImpl, error) {
	c, err := GetOrCreateClient(ctx, dbURI)
	if err != nil {
		return nil, err
	}
	return &SpannerClientImpl{spannerClient: c}, nil
}

func (c *SpannerClientImpl) Refresh(ctx context.Context, dbURI string) error {
	var err error
	c.spannerClient, err = CreateClient(ctx, dbURI)
	if err != nil {
		return err
	}
	return nil
}

func (c *SpannerClientImpl) Single() ReadOnlyTransaction {
	rotxn := c.spannerClient.ReadOnlyTransaction()
	return &ReadOnlyTransactionImpl{rotxn: rotxn}
}

func (c *SpannerClientImpl) DatabaseName() string {
	return c.spannerClient.DatabaseName()
}

type ReadOnlyTransactionImpl struct {
	rotxn *spanner.ReadOnlyTransaction
}

func (ro *ReadOnlyTransactionImpl) Query(ctx context.Context, stmt spanner.Statement) RowIterator {
	ri := ro.rotxn.Query(ctx, stmt)
	return &RowIteratorImpl{ri: ri}
}

type RowIteratorImpl struct {
	ri *spanner.RowIterator
}

func (ri *RowIteratorImpl) Next() (*spanner.Row, error) {
	return ri.ri.Next()
}

func (ri *RowIteratorImpl) Stop() {
	ri.ri.Stop()
}