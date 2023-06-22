package server

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/ridge/limestone/kafka/api"
)

// Handler returns an HTTP handler that servers data from the given Kafka client
// in the same format as the file-based implementation in lib/kafka/local.
//
// Data is served as plain HTTP (read once to the end) or WebSocket (tailing),
// client's choice.
func Handler(client api.Client) http.Handler {
	h := handler{client: client}

	router := mux.NewRouter()
	router.Path("/kafka").Methods(http.MethodGet).HandlerFunc(h.topics)
	router.Path("/kafka/{topic}").Methods(http.MethodGet).HandlerFunc(h.topic)
	router.Path("/kafka/{topic}").Methods(http.MethodHead).HandlerFunc(h.topicInfo)
	router.Path("/kafka/{topic}/{offset}").Methods(http.MethodGet).HandlerFunc(h.record)
	return router
}

type handler struct {
	client api.Client
}
