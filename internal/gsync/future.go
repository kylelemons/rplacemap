package gsync

import (
	"context"
)

type Future[T any] struct {
	ready chan struct{}
	value T
	err   error
}

func FutureOf[T any]() *Future[T] {
	return &Future[T]{
		ready: make(chan struct{}),
	}
}

func (f *Future[T]) Wait(ctx context.Context) (T, error) {
	var zero T
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-f.ready:
		return f.value, f.err
	}
}

func (f *Future[T]) Provide(value T, err error) (T, error) {
	f.value, f.err = value, err
	close(f.ready)
	return value, err
}

func After[T1, T2 any](f1 *Future[T1], xfrm func(T1) (T2, error)) *Future[T2] {
	f2 := FutureOf[T2]()
	go func() {
		<-f1.ready
		if f1.err != nil {
			var zero T2
			f2.Provide(zero, f1.err)
			return
		}
		f2.Provide(xfrm(f1.value))
	}()
	return f2
}
