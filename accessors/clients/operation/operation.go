package operation

import (
	"context"

	"github.com/googleapis/gax-go/v2"
)

// Generic interface for mocking long running operations like CreateStreamOperation, UpdateStreamOperation etc.
type Operation[T any] interface {
	Wait(ctx context.Context, opts ...gax.CallOption) (*T, error)
}

// Wrapping the operation interace in a struct helps us stick to the golang idiom of not returning an interface.
type OperationWrapper[T any] struct {
	elem Operation[T]
}

func (o *OperationWrapper[T]) Wait(ctx context.Context, opts ...gax.CallOption) (*T, error) {
	return o.elem.Wait(ctx, opts...)
}

func NewOperationWrapper[T any](elem Operation[T]) OperationWrapper[T] {
	return OperationWrapper[T]{elem}
}

// Generic interface for mocking long running operations like CreateStreamOperation, UpdateStreamOperation etc.
type NilOperation interface {
	Wait(ctx context.Context, opts ...gax.CallOption) (error)
}

// Wrapping the operation interace in a struct helps us stick to the golang idiom of not returning an interface.
type NilOperationWrapper struct {
	elem NilOperation
}

func (o *NilOperationWrapper) Wait(ctx context.Context, opts ...gax.CallOption) (error) {
	return o.elem.Wait(ctx, opts...)
}

func NewNilOperationWrapper(elem NilOperation) NilOperationWrapper {
	return NilOperationWrapper{elem}
}
