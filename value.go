package dot

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"
)

// Value lists the types supported by Get, Set, and Update. Records
// retain their type: Get and Update require the stored type, while Set may
// replace it. Fixed-width integers use one, two, four, or eight little-endian
// bytes; int and uint always use eight bytes on disk. Float32 and float64 use
// their IEEE 754 bits in the same byte order. time.Duration uses eight bytes
// of signed nanoseconds under its own tag, distinct from int64. Boolean values
// are encoded entirely in the record's type tag. Strings are stored as bytes
// under a distinct tag: a string value is not readable as []byte and vice versa.
// Reading an int or uint that does not fit the host type returns ErrValueOverflow.
type Value interface {
	[]byte | string | bool |
		int | int8 | int16 | int32 | int64 |
		uint | uint8 | uint16 | uint32 | uint64 |
		float32 | float64 | time.Duration
}

// Number lists the value types supported by Add and Sub. Arithmetic retains
// the stored type, including the distinct type of time.Duration.
type Number interface {
	int | int8 | int16 | int32 | int64 |
		uint | uint8 | uint16 | uint32 | uint64 |
		float32 | float64 | time.Duration
}

// ErrInvalidValue indicates a malformed record type or value encoding.
var ErrInvalidValue = errors.New("invalid value encoding")

// ErrTypeMismatch indicates that the stored value has a different type from
// the one requested by Get, Update, Add, or Sub. No implicit conversions are performed.
var ErrTypeMismatch = errors.New("value type mismatch")

// ErrValueOverflow indicates that a stored int or uint does not fit the host's
// int or uint type, or that Add or Sub would overflow or underflow an integer.
// The disk encodings of int and uint are always 64 bits, even on 32-bit hosts.
var ErrValueOverflow = errors.New("value overflows requested type")

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
	typeInt8      recordType = 0x09
	typeInt16     recordType = 0x0a
	typeInt32     recordType = 0x0b
	typeUint8     recordType = 0x0c
	typeUint16    recordType = 0x0d
	typeUint32    recordType = 0x0e
	typeFloat32   recordType = 0x0f
	typeInt       recordType = 0x10
	typeUint      recordType = 0x11
	typeDuration  recordType = 0x12
)

func (ty recordType) String() string {
	switch ty {
	case typeBytes:
		return "[]byte"
	case typeString:
		return "string"
	case typeInt:
		return "int"
	case typeInt8:
		return "int8"
	case typeInt16:
		return "int16"
	case typeInt32:
		return "int32"
	case typeInt64:
		return "int64"
	case typeUint:
		return "uint"
	case typeUint8:
		return "uint8"
	case typeUint16:
		return "uint16"
	case typeUint32:
		return "uint32"
	case typeUint64:
		return "uint64"
	case typeFloat32:
		return "float32"
	case typeFloat64:
		return "float64"
	case typeDuration:
		return "time.Duration"
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
	var want int
	switch ty {
	case typeBytes, typeString:
		return nil
	case typeInt8, typeUint8:
		want = 1
	case typeInt16, typeUint16:
		want = 2
	case typeInt32, typeUint32, typeFloat32:
		want = 4
	case typeInt, typeUint, typeInt64, typeUint64, typeFloat64, typeDuration:
		want = 8
	case typeBoolFalse, typeBoolTrue, typeDelete:
	default:
		return fmt.Errorf("%w: invalid record type 0x%02x", ErrInvalidValue, byte(ty))
	}
	if size != uint64(want) {
		return scalarSizeError(ty.String(), want, int(size))
	}
	if ty == typeDelete && expiration != 0 {
		return fmt.Errorf("%w: invalid record: delete has expiration", ErrInvalidValue)
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
	case int:
		requested = typeInt
	case int8:
		requested = typeInt8
	case int16:
		requested = typeInt16
	case int32:
		requested = typeInt32
	case int64:
		requested = typeInt64
	case uint:
		requested = typeUint
	case uint8:
		requested = typeUint8
	case uint16:
		requested = typeUint16
	case uint32:
		requested = typeUint32
	case uint64:
		requested = typeUint64
	case float32:
		requested = typeFloat32
	case float64:
		requested = typeFloat64
	case time.Duration:
		requested = typeDuration
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
	case int:
		binary.LittleEndian.PutUint64(buf[:], uint64(v))
		return typeInt, buf[:], nil
	case int8:
		buf[0] = byte(v)
		return typeInt8, buf[:1], nil
	case int16:
		binary.LittleEndian.PutUint16(buf[:2], uint16(v))
		return typeInt16, buf[:2], nil
	case int32:
		binary.LittleEndian.PutUint32(buf[:4], uint32(v))
		return typeInt32, buf[:4], nil
	case int64:
		binary.LittleEndian.PutUint64(buf[:], uint64(v))
		return typeInt64, buf[:], nil
	case uint:
		binary.LittleEndian.PutUint64(buf[:], uint64(v))
		return typeUint, buf[:], nil
	case uint8:
		buf[0] = v
		return typeUint8, buf[:1], nil
	case uint16:
		binary.LittleEndian.PutUint16(buf[:2], v)
		return typeUint16, buf[:2], nil
	case uint32:
		binary.LittleEndian.PutUint32(buf[:4], v)
		return typeUint32, buf[:4], nil
	case uint64:
		binary.LittleEndian.PutUint64(buf[:], v)
		return typeUint64, buf[:], nil
	case float32:
		binary.LittleEndian.PutUint32(buf[:4], math.Float32bits(v))
		return typeFloat32, buf[:4], nil
	case float64:
		binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v))
		return typeFloat64, buf[:], nil
	case time.Duration:
		binary.LittleEndian.PutUint64(buf[:], uint64(v))
		return typeDuration, buf[:], nil
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
	switch len(value) {
	case 1:
		result.bits = uint64(value[0])
	case 2:
		result.bits = uint64(binary.LittleEndian.Uint16(value))
	case 4:
		result.bits = uint64(binary.LittleEndian.Uint32(value))
	case 8:
		result.bits = binary.LittleEndian.Uint64(value)
	}
	return result
}

// decodeValue preserves the stored type for Scan. Byte slices borrow the
// record buffer; strings own their bytes. Scalar decoding shares Get's checks,
// including native integer overflow on 32-bit hosts.
func decodeValue(ty recordType, payload []byte) (any, error) {
	switch ty {
	case typeBytes:
		return payload, nil
	case typeString:
		return string(payload), nil
	}
	raw := scalarBytes(ty, payload)
	switch ty {
	case typeInt:
		return decodeScalar[int](raw)
	case typeInt8:
		return decodeScalar[int8](raw)
	case typeInt16:
		return decodeScalar[int16](raw)
	case typeInt32:
		return decodeScalar[int32](raw)
	case typeInt64:
		return decodeScalar[int64](raw)
	case typeUint:
		return decodeScalar[uint](raw)
	case typeUint8:
		return decodeScalar[uint8](raw)
	case typeUint16:
		return decodeScalar[uint16](raw)
	case typeUint32:
		return decodeScalar[uint32](raw)
	case typeUint64:
		return decodeScalar[uint64](raw)
	case typeFloat32:
		return decodeScalar[float32](raw)
	case typeFloat64:
		return decodeScalar[float64](raw)
	case typeDuration:
		return decodeScalar[time.Duration](raw)
	case typeBoolFalse, typeBoolTrue:
		return decodeScalar[bool](raw)
	default:
		return nil, fmt.Errorf("%w: invalid value type 0x%02x", ErrInvalidValue, byte(ty))
	}
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
	case *int:
		v := int64(raw.bits)
		if int64(int(v)) != v {
			return value, fmt.Errorf("%w: %d does not fit in int", ErrValueOverflow, v)
		}
		*out = int(v)
	case *int8:
		*out = int8(raw.bits)
	case *int16:
		*out = int16(raw.bits)
	case *int32:
		*out = int32(raw.bits)
	case *int64:
		*out = int64(raw.bits)
	case *uint:
		if uint64(uint(raw.bits)) != raw.bits {
			return value, fmt.Errorf("%w: %d does not fit in uint", ErrValueOverflow, raw.bits)
		}
		*out = uint(raw.bits)
	case *uint8:
		*out = uint8(raw.bits)
	case *uint16:
		*out = uint16(raw.bits)
	case *uint32:
		*out = uint32(raw.bits)
	case *uint64:
		*out = raw.bits
	case *float32:
		*out = math.Float32frombits(uint32(raw.bits))
	case *float64:
		*out = math.Float64frombits(raw.bits)
	case *time.Duration:
		*out = time.Duration(raw.bits)
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
