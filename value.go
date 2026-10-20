package dot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// Value lists the encodings supported by Get and Set.
// Integers use eight little-endian bytes; float64 uses its IEEE 754 bits in the
// same byte order. A bool uses one byte, either 0 or 1. Values have no type tags.
type Value interface {
	[]byte | int64 | uint64 | float64 | bool
}

// ErrInvalidValue indicates that stored bytes cannot be decoded as the requested
// type. It does not distinguish types with the same valid byte representation.
var ErrInvalidValue = errors.New("invalid value encoding")

type scalarValue struct {
	bits uint64
	size int
}

func scalarBytes(value []byte) scalarValue {
	result := scalarValue{size: len(value)}
	switch len(value) {
	case 1:
		result.bits = uint64(value[0])
	case 8:
		result.bits = binary.LittleEndian.Uint64(value)
	}
	return result
}

func decodeScalar[T Value](raw scalarValue) (T, error) {
	var value T
	switch out := any(&value).(type) {
	case *int64:
		if raw.size != 8 {
			return value, scalarSizeError("int64", 8, raw.size)
		}
		*out = int64(raw.bits)
	case *uint64:
		if raw.size != 8 {
			return value, scalarSizeError("uint64", 8, raw.size)
		}
		*out = raw.bits
	case *float64:
		if raw.size != 8 {
			return value, scalarSizeError("float64", 8, raw.size)
		}
		*out = math.Float64frombits(raw.bits)
	case *bool:
		if raw.size != 1 {
			return value, scalarSizeError("bool", 1, raw.size)
		}
		if raw.bits > 1 {
			return value, fmt.Errorf("%w: bool must be 0 or 1", ErrInvalidValue)
		}
		*out = raw.bits == 1
	default:
		return value, ErrInvalidValue
	}
	return value, nil
}

func scalarSizeError(name string, want, got int) error {
	return fmt.Errorf("%w: %s requires %d bytes, got %d", ErrInvalidValue, name, want, got)
}
