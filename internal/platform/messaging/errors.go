package messaging

import (
	"errors"
	"fmt"
)

// ErrHandlerPanic is wrapped, marked Permanent, by Consume when a handler
// panics; broker packages report it to the subscription's OnErrorFunc.
var ErrHandlerPanic = errors.New("message handler panicked")

type (
	permanentError struct{ err error }

	// BatchError reports a partially failed batch publish: Failed maps the
	// index of each failed message in the published batch to its error, and
	// the remaining messages were delivered. MessagePublisher implementations
	// return it so callers such as the outbox relay retry only the failures;
	// a plain error keeps meaning nothing is assumed delivered.
	BatchError struct {
		Failed map[int]error
	}
)

// Permanent marks err as permanent: processing or delivering the message
// again cannot succeed, so the broker must not redeliver it (dead-lettering
// it where configured). Handle marks payload decoding failures permanent;
// handlers should mark the business errors no retry can fix.
func Permanent(err error) error {
	if err == nil {
		return nil
	}

	return &permanentError{err: err}
}

// IsPermanent reports whether any error in err's tree was marked Permanent.
func IsPermanent(err error) bool {
	var pErr *permanentError
	return errors.As(err, &pErr)
}

func (e *permanentError) Error() string { return "permanent: " + e.err.Error() }

func (e *permanentError) Unwrap() error { return e.err }

func (e *BatchError) Error() string {
	return fmt.Sprintf("%d messages of the batch failed to publish", len(e.Failed))
}

// Unwrap exposes the per-message errors to errors.Is and errors.As.
func (e *BatchError) Unwrap() []error {
	errs := make([]error, 0, len(e.Failed))
	for _, err := range e.Failed {
		errs = append(errs, err)
	}

	return errs
}
