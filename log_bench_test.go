package dot

import (
	"bytes"
	"fmt"
	"testing"
)

func BenchmarkLogWrite(b *testing.B) {
	for _, size := range []int{157, logWriteBufferSize, logWriteBufferSize + 1, 64 << 10} {
		b.Run(fmt.Sprintf("%dB", size), func(b *testing.B) {
			b.StopTimer()
			log, err := newLog(b.TempDir())
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() {
				if err := log.headFile.Close(); err != nil {
					b.Error(err)
				}
			})
			key := bytes.Repeat([]byte{'k'}, 16)
			value := bytes.Repeat([]byte{'v'}, size-13-len(key))
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for done := 0; done < b.N; {
				if err := log.headFile.Truncate(0); err != nil {
					b.Fatal(err)
				}
				log.size = 0
				n := min(4096, logSegmentSize/size, b.N-done)
				b.StartTimer()
				for i := 0; i < n; i++ {
					if _, _, err := log.write('s', key, value); err != nil {
						b.Fatal(err)
					}
				}
				b.StopTimer()
				done += n
			}
		})
	}
}
