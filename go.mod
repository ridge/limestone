module github.com/ridge/limestone

// Long-term fork, as Google is unlikely to remove AppEngine dependency from the library
// https://github.com/golang/oauth2/issues/615
replace golang.org/x/oauth2 => github.com/ridge/oauth2 v0.0.0-20221226133230-d000b8ba2a50

go 1.20

require (
	github.com/fsnotify/fsnotify v1.6.0
	github.com/gorilla/handlers v1.5.1
	github.com/gorilla/mux v1.8.0
	github.com/gorilla/websocket v1.5.0
	github.com/hashicorp/go-memdb v1.3.4
	github.com/kevinpollet/nego v0.0.0-20211010160919-a65cd48cee43
	github.com/ridge/must/v2 v2.0.0
	github.com/ridge/parallel v0.1.1
	github.com/ridge/tj v0.3.0
	github.com/segmentio/kafka-go v0.4.40
	github.com/spf13/pflag v1.0.5
	github.com/stretchr/testify v1.8.4
	go.uber.org/zap v1.24.0
	golang.org/x/crypto/x509roots/fallback v0.0.0-20230622143659-64c3993f5c82
	golang.org/x/exp v0.0.0-20230522175609-2e198f4a06a1
	golang.org/x/oauth2 v0.9.0
	golang.org/x/sys v0.9.0
	golang.org/x/term v0.9.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/felixge/httpsnoop v1.0.1 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.0 // indirect
	github.com/hashicorp/golang-lru v0.5.4 // indirect
	github.com/klauspost/compress v1.15.9 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/pierrec/lz4/v4 v4.1.15 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/stretchr/objx v0.5.0 // indirect
	go.uber.org/atomic v1.7.0 // indirect
	go.uber.org/multierr v1.6.0 // indirect
	golang.org/x/net v0.11.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
