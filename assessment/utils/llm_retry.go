package utils

import (
	"context"
	"math"
	"strings"
	"time"

	"cloud.google.com/go/vertexai/genai"
	"go.uber.org/zap"
)

type LLMRetryClient interface {
	GenerateContentWithRetry(ctx context.Context, model *genai.GenerativeModel, prompt genai.Part, maxRetries int,
		logger *zap.Logger) (*genai.GenerateContentResponse, error)
}

type DefaultLLMRetryClient struct{}

// GenerateContentWithRetry wraps a Vertex AI LLM call with retry logic for rate limiting/quota errors.
func (c *DefaultLLMRetryClient) GenerateContentWithRetry(
	ctx context.Context,
	model *genai.GenerativeModel,
	prompt genai.Part,
	maxRetries int,
	logger *zap.Logger,
) (*genai.GenerateContentResponse, error) {
	var resp *genai.GenerateContentResponse
	var err error
	for i := 0; i < maxRetries; i++ {
		resp, err = model.GenerateContent(ctx, prompt)
		if err == nil {
			return resp, nil
		}
		if strings.Contains(err.Error(), "ResourceExhausted") || strings.Contains(err.Error(), "429") {
			backoff := time.Duration(math.Pow(2, float64(i))) * time.Second
			logger.Warn("Vertex AI rate limited, backing off", zap.Int("attempt", i+1), zap.Duration("backoff", backoff), zap.Error(err))
			time.Sleep(backoff)
			continue
		}
		// For other errors, break early
		break
	}
	return nil, err
}
