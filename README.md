# Dot

A small log-backed key-value store in Go.


## Benchmarks

goos: linux
goarch: amd64
pkg: dot
cpu: AMD Ryzen 7 7840HS w/ Radeon 780M Graphics     
BenchmarkSet-16              	 1500676	       791.6 ns/op	       0 B/op	       0 allocs/op
BenchmarkGetBytesHit-16      	 1679134	       769.3 ns/op	     144 B/op	       1 allocs/op
BenchmarkReplace-16          	  804496	      1463 ns/op	       0 B/op	       0 allocs/op
BenchmarkSetUnchanged-16     	 1756438	       676.2 ns/op	       0 B/op	       0 allocs/op
BenchmarkGetBytesMiss-16     	98267019	        12.21 ns/op	       0 B/op	       0 allocs/op
BenchmarkGetUint64Hit-16     	 2151100	       555.8 ns/op	       0 B/op	       0 allocs/op
BenchmarkGetUint64Miss-16    	100000000	        11.41 ns/op	       0 B/op	       0 allocs/op
BenchmarkSetUint64-16        	 1690581	       690.2 ns/op	       0 B/op	       0 allocs/op
BenchmarkDeleteHit-16        	  915912	      1313 ns/op	       0 B/op	       0 allocs/op
BenchmarkDeleteMiss-16       	87310990	        13.63 ns/op	       0 B/op	       0 allocs/op
PASS
ok  	dot	19.715s