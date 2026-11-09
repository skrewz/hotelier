package server

import (
	"io"
	"net/http"
)

// quietScrapes wraps the /metrics handler for an unauthenticated endpoint.
// When a gather fails, promhttp answers 500 with the raw error string in the
// body — and a collector panic recovered into a GatherError embeds a stack
// trace in that string (function names, file paths). The error belongs in
// the server log, which HandlerOpts.ErrorLog already writes; the anonymous
// caller gets a generic line instead (issue #260).
func quietScrapes(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&quietErrorBodyWriter{ResponseWriter: w}, r)
	})
}

// quietErrorBodyWriter replaces the body of 5xx responses and passes every
// other response through untouched. The status code itself is preserved, so
// a scraper still sees the failure.
type quietErrorBodyWriter struct {
	http.ResponseWriter
	failed bool
	said   bool
}

func (w *quietErrorBodyWriter) WriteHeader(status int) {
	w.failed = status >= http.StatusInternalServerError
	w.ResponseWriter.WriteHeader(status)
}

func (w *quietErrorBodyWriter) Write(b []byte) (int, error) {
	if !w.failed {
		return w.ResponseWriter.Write(b)
	}
	if w.said {
		return len(b), nil
	}
	w.said = true
	_, err := io.WriteString(w.ResponseWriter, "an error occurred while serving metrics\n")
	return len(b), err
}

// Unwrap lets http.ResponseController reach the underlying writer, which
// promhttp uses for flushing and deadlines.
func (w *quietErrorBodyWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
