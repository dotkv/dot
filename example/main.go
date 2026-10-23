package main

import (
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/dotkv/dot"
)

const workers = 1 << 4

func check(err error) {
	if err != nil && err != dot.ErrNotFound {
		panic(err)
	}
}

func main() {
	db, err := dot.Open("db")
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
	for range workers - 1 {
		go work(db)
	}
	work(db)
}

func work(db *dot.DB) {
	go sets(db)
	go deletes(db)
	go updates(db)
	gets(db)
}

func updates(db *dot.DB) {
	for {
		key := fmt.Sprintf("demo-%d", rand.IntN(1<<18))
		check(db.Update(key, func(v uint64, exists bool) (uint64, error) {
			if !exists {
				return 0, dot.ErrNotFound // Abort instead of inserting an empty value.
			}
			return v + 1, nil
		}))
	}
}

func deletes(db *dot.DB) {
	for {
		key := fmt.Sprintf("demo-%d", rand.IntN(1<<18))
		check(db.Delete(key))
	}
}

func gets(db *dot.DB) {
	for {
		key := fmt.Sprintf("demo-%d", rand.IntN(1<<18))
		_, err := db.Get[uint64](key)
		check(err)
	}
}

func sets(db *dot.DB) {
	for {
		n := rand.Uint64N(1 << 18)
		key := fmt.Sprintf("demo-%d", n)
		ext := time.Second.Nanoseconds() + rand.Int64N(time.Second.Nanoseconds())
		exp := time.Now().Add(time.Duration(ext))
		check(db.Set(key, n, dot.WithExpiration(exp)))
	}
}
