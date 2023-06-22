// Package thttp contains HTTP server and client utilities.
//
// # HTTP Server
//
// Most use cases for http.Server are covered by thttp.Server with the following
// advantages:
//
// * Instead of pre-context-era start-and-stop paradigm, thttp.Server is
// controlled with a context passed to its Run method. This fits much better
// into hierarchies of internal components that need to be started and shut down
// as a whole. Plays especially nice with parallel.Run.
//
// * The server code ensures that every incoming request has a context inherited
// from the context passed to Run, thus supporting the global expectation that
// every context contains a logger.
//
// * The somewhat tricky graceful shutdown sequence is taken care of by
// thttp.Server.
//
// Note that only a single handler is passed to thttp.NewServer as its second
// argument. Most use cases will need path-based routing. The standard solution
// is to use github.com/gorilla/mux as in the example below.
//
// # Example
//
// thttp.Server fits best into components that themselves are
// context-controlled. The following example demonstrates the use of the
// parallel library to run both an HTTP server and a Limestone instance. When
// the parent context is closed, RunDataServer will return after graceful
// shutdown of both sub-components.
//
//	func RunDataServer(ctx context.Context, addr string) error {
//	    return parallel.Run(ctx, func(ctx context.Context, spawn parallel.SpawnFn) error {
//	        db := limestone.New(...)
//	        spawn("limestone", parallel.Fail, db.Run)
//
//	        // Creating the listener early allows incoming connections to be queued
//	        // while Limestone is catching up
//	        listener, err := tnet.Listen(addr)
//	        if err != nil {
//	            return errors.Wrap(err, "failed to run data server")
//	        }
//
//	        err := db.WaitReady(ctx)
//	        if err != nil {
//	            return errors.Wrap(err, "failed to run data server")
//	        }
//
//	        router := mux.NewRouter()
//	        router.HandleFunc("/data/{id}", getHandler(db)).Methods(http.MethodGet)
//	        router.HandleFunc("/data/{id}", putHandler(db)).Methods(http.MethodPut)
//
//	        server := thttp.NewServer(listener,
//	            thttp.Wrap(router, thttp.StandardMiddleware, thttp.LogBodies))
//	        spawn("http", parallel.Fail, server.Run)
//
//	        return nil
//	    })
//	}
//
// # Middleware
//
// A middleware is a function that takes an http.Handler and returns an
// http.Handler, usually wrapping the handler with code that runs before, after
// or even instead of the one being wrapped.
//
// One can use a middleware to wrap a handler manually:
//
//	handler = thttp.CORS(handler)
//
// A middleware can also be applied to the mux router or a sub-router:
//
//	router.Use(thttp.CORS)
//
// To apply a handler to all requests handled by the server, which is
// the most common use case, it's convenient to use thttp.Wrap function.
// This function takes any number of middleware, which are applied in
// order so that the first one listed is the first to see the incoming request.
//
//	server := thttp.NewServer(listener,
//	    thttp.Wrap(handler, thttp.StandardMiddleware, thttp.LogBodies))
//
// It is recommended to include at least thttp.StandardMiddleware, and put it
// first. This single middleware is equivalent to listing thttp.Log,
// thttp.Recover and thttp.CORS, in this order. It does not include LogBodies
// because its use is less universal.
//
// # Request context
//
// In an HTTP handler, r.Context() returns the request context. It is a
// descendant of the context passed into the Run method of thttp.Server, and
// contains all the values stored there. However, during shutdown it will stay
// open for somewhat longer than the parent context to allow current running
// requests to complete.
//
// # Logging guidelines
//
// For all logging in HTTP handlers, use the logger embedded in the request
// context:
//
//	logger := tlog.Get(r.Context())
//
// This logger contains the following structured fields:
//
// * httpServer: the local listening address (to distinguish between messages
// from several HTTP servers)
//
// * remoteAddr: the IP address and port of the remote client
//
// If the thttp.Log middleware is installed, which it should be (usually via
// thttp.StandardMiddleware), this logger will also contain the following
// fields:
//
// * requestID: a unique ID generated for the request to tie the log messages
// together
//
// * method, host, url: self-explanatory
//
// Please avoid redundant logging. In particular:
//
// * Don't log any of the information above explicitly.
//
// * Don't include generic unconditional log messages at the beginning and end
// of your handler unless they contain informative fields. The thttp.Log
// middleware already logs before and after handling of each request.
//
// * If you need to log all request and response bodies (at Debug level), use
// the thttp.LogBodies middleware.
//
// * In case of an internal error, don't log it explicitly. Just panic, and the
// thttp.Recover middleware will log the complete error with the panic stack.
// The client will receive a generic error 500 (unless the headers have already
// been sent), without the details of the error being exposed. Afterwards,
// the server will be terminated gracefully.
package thttp
