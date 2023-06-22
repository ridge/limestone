package server

import (
	"net/http"
	"strings"

	"github.com/ridge/limestone/tlog"
	"go.uber.org/zap"
)

func (h handler) topics(w http.ResponseWriter, r *http.Request) {
	topics, err := h.client.Topics(r.Context())
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		if _, err := w.Write([]byte(err.Error())); err != nil {
			tlog.Get(r.Context()).Info("Failed to write response", zap.Error(err))
			return
		}
	}
	if _, err := w.Write([]byte(strings.Join(topics, "\n") + "\n")); err != nil {
		tlog.Get(r.Context()).Info("Failed to write response", zap.Error(err))
	}
}
