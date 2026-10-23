package dot

import (
	"bytes"
	"errors"
	"fmt"
)

// ErrConditionFailed is returned directly when a WithCondition predicate returns
// false. The mutation is not applied, and its durability option is not performed.
var ErrConditionFailed = errors.New("condition failed")

// WithCondition permits a mutation only when fn returns true for the current
// value. It applies to Set, Update, Add, Sub, and Delete; Open rejects it.
// Missing or expired keys pass the zero value of T and exists == false. An
// existing value must have type T or the operation returns an error wrapping
// ErrTypeMismatch without calling fn. Set may write a different type afterward.
// The last WithCondition option wins. A nil predicate returns an error.
//
// The predicate runs once under the shard's exclusive lock, before any update
// callback or arithmetic. It must be short and must not call methods on this DB.
// Byte slices passed to fn are owned by the caller and may be changed or retained.
// Returning false yields ErrConditionFailed without changing the key or its
// expiration. A panic propagates without applying the mutation. Storage errors
// cannot undo external effects of a predicate that has already run.
func WithCondition[T Value](fn func(current T, exists bool) bool) Option {
	condition := func(raw scalarValue, payload []byte, exists bool) error {
		if fn == nil {
			return errors.New("condition callback must not be nil")
		}
		var value T
		if exists {
			if err := checkType[T](raw.ty); err != nil {
				return fmt.Errorf("condition: %w", err)
			}
			switch out := any(&value).(type) {
			case *[]byte:
				*out = bytes.Clone(payload)
			case *string:
				*out = string(payload)
			default:
				var err error
				value, err = decodeScalar[T](raw)
				if err != nil {
					return fmt.Errorf("condition: %w", err)
				}
			}
		}
		if !fn(value, exists) {
			return ErrConditionFailed
		}
		return nil
	}
	return func(o *Options) {
		o.condition = condition
	}
}

// checkCondition reads the previous value for Set and Delete while their caller
// holds the shard lock. Update supplies its already-read value directly.
func (o Options) checkCondition(s *Shard, key []byte, hash uint64) error {
	if o.condition == nil {
		return nil
	}
	pos, payload, ty, expiration, err := s.index.lookupPayload(s.log, key, hash, s.log.writeBuffer[:], true)
	if err != nil {
		return fmt.Errorf("condition: %w", err)
	}
	exists := pos >= 0 && !isExpired(expiration)
	return o.condition(scalarBytes(ty, payload), payload, exists)
}
