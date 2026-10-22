package main

import (
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"time"

	"github.com/dotkv/dot"
)

func check(err error) {
	if err != nil && err != dot.ErrNotFound {
		panic(err)
	}
}

func main() {
	path, err := os.MkdirTemp("", "dot-example-")
	check(err)
	defer os.RemoveAll(path)
	db, err := dot.Open(path,
		dot.WithShardCount(16),
		dot.WithCompactionThreshold(0.33, 32<<10),
	)
	check(err)
	defer db.Destroy()

	mux := http.NewServeMux()
	uipath := "/dot/metrics"
	mux.Handle(uipath+"/", http.StripPrefix(uipath, db.MetricsHandler()))
	go func() {
		addr := "localhost:8080"
		log.Printf("Dashboard: http://%s%s/", addr, uipath)
		check(http.ListenAndServe(addr, mux))
	}()
	value := make([]byte, 1024)
	for i := range uint64(1<<64 - 1) {
		key := fmt.Sprintf("demo-%d", rand.IntN(1<<16))
		switch i % 10 {
		case 0, 1, 2, 3, 4, 5, 6:
			_, err := db.Get[[]byte](key)
			check(err)
		case 7:
			ext := time.Second.Nanoseconds() + rand.Int64N(time.Second.Nanoseconds())
			exp := time.Now().Add(time.Duration(ext))
			check(db.Set(key, value, dot.WithExpiration(exp)))
		case 8:
			check(db.Delete(key))
		case 9:
			check(db.Update(key, func(v []byte, exists bool) ([]byte, error) {
				if !exists {
					return value, nil
				}
				v[0]++
				return v, nil
			}))
		}
	}
}
