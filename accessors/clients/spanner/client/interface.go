package spannerclient

import (
	"context"

	sp "cloud.google.com/go/spanner"
)


type SpannerClient interface {}

// This implements the SpannerClient interface. This is the primary implementation that should be used in all places other than tests.
type SpannerCLientImpl struct {
	spannerClient *sp.Client
}

func NewSpannerClientImpl(ctx context.Context, dbURI string) (*SpannerCLientImpl, error) {
	c, err := GetOrCreateClient(ctx, dbURI)
	if err != nil {
		return nil, err
	}
	return &SpannerCLientImpl{spannerClient: c}, nil
}