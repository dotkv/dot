package dot

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"testing"
)

func TestLogRejectsInvalidTypedRecords(t *testing.T) {
	cases := []struct {
		name       string
		ty         recordType
		value      []byte
		expiration int64
	}{
		{"zero-type", 0, nil, 0},
		{"unknown-type", 0xff, nil, 0},
		{"false-payload", typeBoolFalse, []byte{0}, 0},
		{"true-payload", typeBoolTrue, []byte{1}, 0},
		{"delete-payload", typeDelete, []byte{0}, 0},
		{"delete-expiration", typeDelete, nil, 1},
	}
	for _, tc := range typedCases() {
		if tc.payload == 0 || tc.logical == "[]byte" {
			continue
		}
		sizes := []int{tc.payload - 1, tc.payload + 1}
		if tc.payload > 1 {
			sizes = append(sizes, 0)
		}
		for _, size := range sizes {
			cases = append(cases, struct {
				name       string
				ty         recordType
				value      []byte
				expiration int64
			}{fmt.Sprintf("%s-size-%d", tc.name, size), tc.ty, make([]byte, size), 0})
		}
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Include long keys to exercise the scalar reader's fallback path.
			for _, keySize := range []int{3, 1100} {
				t.Run(fmt.Sprintf("key=%d", keySize), func(t *testing.T) {
					key := bytes.Repeat([]byte{'k'}, keySize)
					data := recoveryRecord(tc.ty, key, tc.value, tc.expiration)
					log, _ := openRecoveryLog(t, t.TempDir())
					if err := os.WriteFile(log.headFile.Name(), data, 0600); err != nil {
						t.Fatal(err)
					}
					for _, buf := range [][]byte{nil, make([]byte, 4096)} {
						if _, _, _, _, err := log.readPayload(0, 0, buf); !errors.Is(err, ErrInvalidValue) {
							t.Fatalf("payload read accepted invalid record: %v", err)
						}
					}
					if _, _, _, err := log.readScalar(0, 0, key); !errors.Is(err, ErrInvalidValue) {
						t.Fatalf("scalar read accepted invalid record: %v", err)
					}
				})
			}
			for _, sealed := range []bool{false, true} {
				t.Run(fmt.Sprintf("recovery/sealed=%t", sealed), func(t *testing.T) {
					path := t.TempDir()
					data := append(recoveryRecord(typeBoolTrue, []byte("previous"), nil, 0),
						recoveryRecord(tc.ty, []byte("bad"), tc.value, tc.expiration)...)
					name := writeRecoverySegment(t, path, 0, data)
					if sealed {
						writeRecoverySegment(t, path, 1, nil)
					}
					log, err := newLog(path, newInternalIndex(t))
					if log != nil {
						log.headFile.Close()
					}
					if log != nil || !errors.Is(err, ErrInvalidValue) {
						t.Fatalf("recovery accepted invalid record: %v", err)
					}
					if got, err := os.ReadFile(name); err != nil || !bytes.Equal(got, data) {
						t.Fatalf("failed recovery changed records: %v", err)
					}
					if count := openFilesUnder(t, path); count != 0 {
						t.Fatalf("failed recovery leaked %d descriptors", count)
					}
				})
			}
		})
	}
}

func TestLogTypedRecordTags(t *testing.T) {
	// Pin the public disk format independently of the recordType constants.
	for _, tc := range []struct {
		ty    recordType
		code  byte
		value []byte
	}{
		{typeDelete, 0x01, nil},
		{typeBytes, 0x02, nil},
		{typeInt64, 0x03, make([]byte, 8)},
		{typeUint64, 0x04, make([]byte, 8)},
		{typeFloat64, 0x05, make([]byte, 8)},
		{typeBoolFalse, 0x06, nil},
		{typeBoolTrue, 0x07, nil},
		{typeString, 0x08, nil},
		{typeInt8, 0x09, make([]byte, 1)},
		{typeInt16, 0x0a, make([]byte, 2)},
		{typeInt32, 0x0b, make([]byte, 4)},
		{typeUint8, 0x0c, make([]byte, 1)},
		{typeUint16, 0x0d, make([]byte, 2)},
		{typeUint32, 0x0e, make([]byte, 4)},
		{typeFloat32, 0x0f, make([]byte, 4)},
		{typeInt, 0x10, make([]byte, 8)},
		{typeUint, 0x11, make([]byte, 8)},
		{typeDuration, 0x12, make([]byte, 8)},
	} {
		log, _ := openRecoveryLog(t, t.TempDir())
		if _, _, err := log.write(tc.ty, nil, tc.value, 0); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(log.headFile.Name())
		if err != nil || uint64(len(data)) != recordSize(0, len(tc.value)) || data[4] != tc.code {
			t.Fatalf("record tag %s: data=%x error=%v", tc.ty, data, err)
		}
	}
}

func TestLogRecoveryIncompleteTypedHead(t *testing.T) {
	for _, tc := range typedCases() {
		t.Run(tc.name, func(t *testing.T) {
			key := []byte("partial")
			complete := recoveryRecord(tc.ty, key, make([]byte, tc.payload), 0)
			for _, length := range []int{logHeaderSize - 1, len(complete) - 1} {
				path := t.TempDir()
				prefix := recoveryRecord(typeBoolFalse, []byte("keep"), nil, 0)
				name := writeRecoverySegment(t, path, 0, append(bytes.Clone(prefix), complete[:length]...))
				log, _ := openRecoveryLog(t, path)
				got, err := os.ReadFile(name)
				if err != nil || !bytes.Equal(got, prefix) || log.size != uint64(len(prefix)) {
					t.Fatalf("incomplete typed record not repaired: size=%d error=%v", log.size, err)
				}
			}
		})
	}
}
