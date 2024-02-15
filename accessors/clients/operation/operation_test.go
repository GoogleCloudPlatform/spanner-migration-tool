package operation

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/GoogleCloudPlatform/spanner-migration-tool/logger"
	"github.com/googleapis/gax-go/v2"
	"go.uber.org/zap"
)

func init() {
	logger.Log = zap.NewNop()
}

func TestMain(m *testing.M) {
	res := m.Run()
	os.Exit(res)
}

type intOperationValue struct {
	val int64
	e   error
}

func (i intOperationValue) Wait(ctx context.Context, opts ...gax.CallOption) (*int64, error) {
	return &i.val, i.e
}

func TestWait(t *testing.T) {
	ctx := context.Background()
	var testVal int64 = 42
	testError := errors.New("testError")
	i := intOperationValue{testVal, testError}
	o := NewOperationWrapper[int64](i)
	v, e := o.Wait(ctx)
	assert.Equal(t, *v, testVal, "operationWrapper.Wait must return correct value")
	assert.Equal(t, e, testError, "operationWrapper.Wait must return correct error")
}
