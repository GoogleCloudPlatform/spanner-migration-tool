package import_data

import (
	"context"
	"flag"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/GoogleCloudPlatform/spanner-migration-tool/cmd"
	"github.com/stretchr/testify/mock"
	"google.golang.org/api/iterator"
)

type MockSpannerClient struct {
	DatabaseNameMock        func() string
	ClientIDMock            func() string
	CloseMock               func()
	SingleMock              func() *MockReadOnlyTransaction
	ReadOnlyTransactionMock func() *MockReadOnlyTransaction
	ApplyMock               func(ctx context.Context, ms []*MutationMock, opts ...ApplyOptionMock)
	BatchWriteMock          func(ctx context.Context, mgs []*MutationGroupMock) *BatchWriteResponseIteratorMock
	mock.Mock
}

type MockReadOnlyTransaction struct {
	mock.Mock
}

type MutationMock struct {
	mock.Mock
}

type ApplyOptionMock struct {
	mock.Mock
}

type MutationGroupMock struct {
	mock.Mock
}

type BatchWriteResponseIteratorMock struct {
	mock.Mock
}

// Single mocks the Single method.
func (m *MockSpannerClient) Single() *spanner.ReadOnlyTransaction {
	args := m.Called()
	return args.Get(0).(*spanner.ReadOnlyTransaction)
}

// Close mocks the Close method.
func (m *MockSpannerClient) Close() {
	m.Called()
}

// Query mocks the Query method.
func (m *MockReadOnlyTransaction) Query(ctx context.Context, stmt spanner.Statement) *spanner.RowIterator {
	args := m.Called(ctx, stmt)
	return args.Get(0).(*spanner.RowIterator)
}

// MockRowIterator is a mock implementation of spanner.RowIterator.
type MockRowIterator struct {
	mock.Mock
}

// Next mocks the Next method.
func (m *MockRowIterator) Next() (*spanner.Row, error) {
	args := m.Called()
	return args.Get(0).(*spanner.Row), args.Error(1)
}

// Stop mocks the Stop method.
func (m *MockRowIterator) Stop() {
	m.Called()
}

// MockRow is a mock implementation of spanner.Row.
type MockRow struct {
	mock.Mock
}

// Columns mocks the Columns method.
func (m *MockRow) Columns(dest ...interface{}) error {
	args := m.Called(dest)
	return args.Error(0)
}

func TestTableExists(t *testing.T) {
	ctx := context.Background()

	t.Run("Table Exists", func(t *testing.T) {
		mockClient := new(MockSpannerClient)
		mockTxn := new(MockReadOnlyTransaction)
		mockIterator := new(MockRowIterator)
		mockRow := new(MockRow)

		mockClient.On("Single").Return(mockTxn)
		mockTxn.On("Query", ctx, mock.AnythingOfType("spanner.Statement")).Return(mockIterator)
		mockIterator.On("Next").Return(mockRow, nil).Once()
		mockIterator.On("Next").Return(nil, iterator.Done).Once()
		mockIterator.On("Stop").Return()

		importDataCmd := cmd.ImportDataCmd{}

		fs := flag.NewFlagSet("testSetFlags", flag.ContinueOnError)
		importDataCmd.SetFlags(fs)

		//sourceCsv := SourceCsvImpl{}
		//sourceCsv.Import(ctx, &importDataCmd)

		//if err != nil {
		//	t.Errorf("Unexpected error: %v", err)
		//}
		//if !exists {
		//	t.Error("Expected table to exist, but it didn't")
		//}
		//mockClient.AssertExpectations(t)
		//mockTxn.AssertExpectations(t)
		//mockIterator.AssertExpectations(t)
	})
}
