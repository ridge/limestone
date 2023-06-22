package thttp

import (
	"net/http"

	"github.com/gorilla/handlers"
)

var (
	allowedMethods = []string{
		http.MethodGet,
		http.MethodPost,
		http.MethodOptions,
		http.MethodPut,
		http.MethodDelete,
		http.MethodPatch,
	}
	allowedHeaders = []string{
		"Authorization",
		"Cache-Control",
		"Content-Type",
		"DNT",
		"If-Modified-Since",
		"Range",
		"User-Agent",
		"X-Requested-With",
		"X-Ridge-Request-ID",
	}
	exposedHeaders = []string{
		"Content-Length",
		"Content-Range",
	}
)

// CORS is a middleware that allows cross-origin requests
var CORS = handlers.CORS(
	handlers.AllowedMethods(allowedMethods),
	handlers.AllowedHeaders(allowedHeaders),
	handlers.ExposedHeaders(exposedHeaders),
	handlers.AllowedOrigins([]string{"*"}), // FIXME (alexey): we should probably tighten this
)
