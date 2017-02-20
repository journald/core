package http

import (
	"log"
	"net/http"
	"time"
)

func trace(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rw := &ResponseWriter{
			Status:         http.StatusOK,
			ResponseWriter: w,
		}

		then := time.Now()
		h.ServeHTTP(rw, r)
		elapsed := time.Since(then)
		log.Printf("method=%s path=%s status=%d duration=%s ip=%s\n", r.Method, r.URL, rw.Status, elapsed, r.RemoteAddr)
	}
}
