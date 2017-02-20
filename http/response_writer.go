package http

import "net/http"

type ResponseWriter struct {
	http.ResponseWriter
	Status int
}

func (r *ResponseWriter) WriteHeader(status int) {
	r.Status = status
	r.ResponseWriter.WriteHeader(status)
}
