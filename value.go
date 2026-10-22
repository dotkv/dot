package dot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
)

// Value lists the types supported by Get, Set, Update, and Upsert. Records
// retain their type: Get and Update require the stored type, while Set may
// replace it. Integers use eight little-endian bytes; float64 uses its IEEE
// 754 bits in the same byte order. Boolean values are encoded entirely in
// the record's type tag. Strings are stored as their bytes under a distinct
// tag: a string value is not readable as []byte and vice versa.
type Value interface {
	[]byte | string | int64 | uint64 | float64 | bool
}

// ErrInvalidValue indicates a malformed record type or value encoding.
var ErrInvalidValue = errors.New("invalid value encoding")

// ErrTypeMismatch indicates that the stored value has a different type from
// the one requested by Get or Update. No implicit conversions are performed.
var ErrTypeMismatch = errors.New("value type mismatch")

type recordType byte

// These explicit values are part of the disk format. Zero is invalid.
const (
	typeDelete    recordType = 0x01
	typeBytes     recordType = 0x02
	typeInt64     recordType = 0x03
	typeUint64    recordType = 0x04
	typeFloat64   recordType = 0x05
	typeBoolFalse recordType = 0x06
	typeBoolTrue  recordType = 0x07
	typeString    recordType = 0x08
)

func (ty recordType) String() string {
	switch ty {
	case typeBytes:
		return "[]byte"
	case typeString:
		return "string"
	case typeInt64:
		return "int64"
	case typeUint64:
		return "uint64"
	case typeFloat64:
		return "float64"
	case typeBoolFalse, typeBoolTrue:
		return "bool"
	case typeDelete:
		return "delete"
	default:
		return "unknown"
	}
}

// validate checks record shape independently of the type requested by a caller.
// Recovery and both normal read paths use the same disk-format rules.
func (ty recordType) validate(size uint64, expiration int64) error {
	switch ty {
	case typeBytes, typeString:
		return nil
	case typeInt64, typeUint64, typeFloat64:
		if size != 8 {
			return scalarSizeError(ty.String(), 8, int(size))
		}
	case typeBoolFalse, typeBoolTrue, typeDelete:
		if size != 0 {
			return scalarSizeError(ty.String(), 0, int(size))
		}
		if ty == typeDelete && expiration != 0 {
			return fmt.Errorf("%w: invalid record: delete has expiration", ErrInvalidValue)
		}
	default:
		return fmt.Errorf("%w: invalid record type 0x%02x", ErrInvalidValue, byte(ty))
	}
	return nil
}

func checkType[T Value](stored recordType) error {
	var value T
	var requested recordType
	switch any(value).(type) {
	case []byte:
		requested = typeBytes
	case string:
		requested = typeString
	case int64:
		requested = typeInt64
	case uint64:
		requested = typeUint64
	case float64:
		requested = typeFloat64
	case bool:
		requested = typeBoolFalse
	}
	logical := stored
	if logical == typeBoolTrue {
		logical = typeBoolFalse
	}
	if logical != requested {
		return fmt.Errorf("%w: stored %s, requested %s", ErrTypeMismatch, stored, requested)
	}
	return nil
}

type scalarValue struct {
	bits uint64
	size int
	ty   recordType
}

// encodeValue borrows buf for numeric scalars and passes byte slices through
// unchanged. Strings are converted once; the record writer copies the result.
// Booleans have no payload; their value is carried by the tag.
func encodeValue[T Value](value T, buf *[8]byte) (recordType, []byte, error) {
	switch v := any(value).(type) {
	case []byte:
		return typeBytes, v, nil
	case string:
		return typeString, []byte(v), nil
	case int64:
		binary.LittleEndian.PutUint64(buf[:], uint64(v))
		return typeInt64, buf[:], nil
	case uint64:
		binary.LittleEndian.PutUint64(buf[:], v)
		return typeUint64, buf[:], nil
	case float64:
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v))
		return typeFloat64, buf[:], nil
	case bool:
		if v {
			return typeBoolTrue, nil, nil
		}
		return typeBoolFalse, nil, nil
	default:
		return 0, nil, ErrInvalidValue
	}
}

func scalarBytes(ty recordType, value []byte) scalarValue {
	result := scalarValue{ty: ty, size: len(value)}
	if len(value) == 8 {
		result.bits = binary.LittleEndian.Uint64(value)
	}
	return result
}

func decodeScalar[T Value](raw scalarValue) (T, error) {
	var value T
	if err := checkType[T](raw.ty); err != nil {
		return value, err
	}
	if err := raw.ty.validate(uint64(raw.size), 0); err != nil {
		return value, err
	}
	switch out := any(&value).(type) {
	case *int64:
		*out = int64(raw.bits)
	case *uint64:
		*out = raw.bits
	case *float64:
		*out = math.Float64frombits(raw.bits)
	case *bool:
		*out = raw.ty == typeBoolTrue
	default:
		return value, ErrInvalidValue
	}
	return value, nil
}

func scalarSizeError(name string, want, got int) error {
	return fmt.Errorf("%w: invalid record: %s requires %d bytes, got %d", ErrInvalidValue, name, want, got)
}
