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

package expressions_api

import (
	"context"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/internal"
)

type MockExpressionVerificationAccessor struct {
	VerifyExpressionsMock    func(ctx context.Context, verifyExpressionsInput internal.VerifyExpressionsInput) internal.VerifyExpressionsOutput
	RefreshSpannerClientMock func(ctx context.Context, project string, instance string) error
}

func (mev *MockExpressionVerificationAccessor) VerifyExpressions(ctx context.Context, verifyExpressionsInput internal.VerifyExpressionsInput) internal.VerifyExpressionsOutput {
	return mev.VerifyExpressionsMock(ctx, verifyExpressionsInput)
}

func (mev *MockExpressionVerificationAccessor) RefreshSpannerClient(ctx context.Context, project string, instance string) error {
	return mev.RefreshSpannerClientMock(ctx, project, instance)
}

type MockDDLVerifier struct {
	VerifySpannerDDLMock            func(conv *internal.Conv, expressionDetails []internal.ExpressionDetail) (internal.VerifyExpressionsOutput, error)
	GetSpannerExpressionDetailsMock func(conv *internal.Conv, tableIds []string) []internal.ExpressionDetail
	GetSourceExpressionDetailsMock  func(conv *internal.Conv, tableIds []string) []internal.ExpressionDetail
	RefreshSpannerClientMock        func(ctx context.Context, project string, instance string) error
}

func (m *MockDDLVerifier) VerifySpannerDDL(conv *internal.Conv, expressionDetails []internal.ExpressionDetail) (internal.VerifyExpressionsOutput, error) {
	if m.VerifySpannerDDLMock != nil {
		return m.VerifySpannerDDLMock(conv, expressionDetails)
	}
	return internal.VerifyExpressionsOutput{}, nil
}

func (m *MockDDLVerifier) GetSpannerExpressionDetails(conv *internal.Conv, tableIds []string) []internal.ExpressionDetail {
	if m.GetSpannerExpressionDetailsMock != nil {
		return m.GetSpannerExpressionDetailsMock(conv, tableIds)
	}
	return []internal.ExpressionDetail{}
}

func (m *MockDDLVerifier) GetSourceExpressionDetails(conv *internal.Conv, tableIds []string) []internal.ExpressionDetail {
	if m.GetSourceExpressionDetailsMock != nil {
		return m.GetSourceExpressionDetailsMock(conv, tableIds)
	}
	return []internal.ExpressionDetail{}
}

func (m *MockDDLVerifier) RefreshSpannerClient(ctx context.Context, project string, instance string) error {
	if m.RefreshSpannerClientMock != nil {
		return m.RefreshSpannerClientMock(ctx, project, instance)
	}
	return nil
}
