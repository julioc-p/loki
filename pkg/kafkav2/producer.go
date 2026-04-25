package kafkav2

import (
	"context"
	"sync/atomic" //lint:ignore faillint we use new atomic types from sync/atomic.

	"github.com/twmb/franz-go/pkg/kgo"
)

type Producer interface {
	Produce(context.Context, *kgo.Record, func(*kgo.Record, error))
}

// A NoCancelProducer wraps a [kgo.Client] but unlike the wrapped client,
// cancelation of a context does not risk cancelation of all buffered records
// for the produced partitions. From the franz-go docs:
//
//	Once a record is buffered into a batch, it can be canceled in three ways:
//	canceling the context, the record timing out, or hitting the maximum
//	retries. If any of these conditions are hit and it is currently safe to
//	fail records, all buffered records for the relevant partition are failed.
//	Only the first record's context in a batch is considered when determining
//	whether the batch should be canceled.
//
// For more information, see
// [https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Client.Produce].
type NoCancelProducer struct {
	client Producer
}

// NewNoCancelProducer returns a new NoCancelProducer.
func NewNoCancelProducer(client Producer) *NoCancelProducer {
	return &NoCancelProducer{client: client}
}

// Produce sends the Kafka record to its requested topic. It calls the optional
// promise on both success and failure, with a non-nil error on failure.
// However, unlike [kgo], a canceled context will not cancel all other buffered
// records for the same partition.
func (p *NoCancelProducer) Produce(ctx context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	var producePromise func(*kgo.Record, error)
	// Slow path. We have a promise, so we need to make sure it is called when
	// the context is canceled.
	if promise != nil {
		// done is closed when the promise has been called.
		done := make(chan struct{})
		// promiseOnce calls promise if it has not been called before, and then closes
		// done. We use an atomic here instead of a [sync.Once] as we do not need to
		// wait for f to complete.
		promiseOnce := atomic.Int64{}
		producePromise = func(r *kgo.Record, err error) {
			if promiseOnce.CompareAndSwap(0, 1) {
				promise(r, err)
				close(done)
			}
		}
		go func() {
			// If the context is canceled, call the promise. If the promise was called
			// from [Produce] instead, return.
			select {
			case <-ctx.Done():
				producePromise(r, ctx.Err())
			case <-done:
				return
			}
		}()
	}
	p.client.Produce(context.WithoutCancel(ctx), r, producePromise)
}

// ProduceSync is the synchronous version of Produce.
func (p *NoCancelProducer) ProduceSync(ctx context.Context, rs ...*kgo.Record) kgo.ProduceResults {
	results := make(kgo.ProduceResults, 0, len(rs))
	if len(rs) == 0 {
		return results
	}
	noCancelCtx := context.WithoutCancel(ctx)
	// done is closed when resultsCh is closed.
	done := make(chan struct{})
	// resultsCh contains a [kgo.ProduceResult] for each record in rs.
	resultsCh := make(chan kgo.ProduceResult, len(rs))
	// rem counts the number of promises still to be called. It ensures that
	// resultsCh is closed once, and after all promises have been executed.
	rem := atomic.Int64{}
	rem.Add(int64(len(rs)))
	for _, r := range rs {
		p.client.Produce(noCancelCtx, r, func(r *kgo.Record, err error) {
			resultsCh <- kgo.ProduceResult{
				Record: r,
				Err:    err,
			}
			if rem.Add(-1) == 0 {
				// This is the last promise, so we must close resultsCh.
				close(resultsCh)
				close(done)
			}
		})
	}
	select {
	case <-ctx.Done():
		// ctx.Err() is not free on a [cancelCtx], call it once.
		ctxErr := ctx.Err()
		for _, r := range rs {
			results = append(results, kgo.ProduceResult{
				Record: r,
				Err:    ctxErr,
			})
		}
	case <-done:
		for res := range resultsCh {
			results = append(results, res)
		}
	}
	return results
}
