package dot

import "fmt"

// Add atomically adds delta to key and returns the new value. A missing or
// expired key starts at zero. The stored type must match delta's type.
// Integer overflow or underflow returns an error wrapping ErrValueOverflow
// without changing the value. Floating-point values follow Go arithmetic,
// including infinities and NaNs. Expiration and durability follow Update;
// metrics count this operation as an update. On error, Add returns zero.
// WithCondition checks the previous value before arithmetic.
func (db *DB) Add[T Number, K Key](key K, delta T, opts ...Option) (T, error) {
	return db.arithmetic(key, delta, false, opts...)
}

// Sub atomically subtracts delta from key and returns the new value. A missing
// or expired key starts at zero; subtracting a positive unsigned delta from a
// missing key returns an error wrapping ErrValueOverflow without inserting it.
// The stored type must match delta's type. Integer overflow or underflow leaves
// the value unchanged; floating-point values follow Go arithmetic. Expiration, durability,
// and metrics follow Add. On error, Sub returns zero.
// WithCondition checks the previous value before arithmetic.
func (db *DB) Sub[T Number, K Key](key K, delta T, opts ...Option) (T, error) {
	return db.arithmetic(key, delta, true, opts...)
}

func (db *DB) arithmetic[T Number, K Key](key K, delta T, subtract bool, opts ...Option) (T, error) {
	var result T
	err := db.Update(key, func(current T, _ bool) (T, error) {
		if subtract {
			result = current - delta
		} else {
			result = current + delta
		}
		switch any(delta).(type) {
		case float32, float64:
			return result, nil
		}
		// Wrapped integer results move in the opposite direction to delta.
		// Subtract directly rather than negating delta, which would overflow
		// for the minimum signed integer and cannot represent unsigned values.
		if subtract {
			if (delta > 0 && result > current) || (delta < 0 && result < current) {
				return 0, fmt.Errorf("subtract %v from %v: %w", delta, current, ErrValueOverflow)
			}
		} else if (delta > 0 && result < current) || (delta < 0 && result > current) {
			return 0, fmt.Errorf("add %v to %v: %w", delta, current, ErrValueOverflow)
		}
		return result, nil
	}, opts...)
	if err != nil {
		return 0, err
	}
	return result, nil
}
