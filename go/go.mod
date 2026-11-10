module github.com/varveio/seekparquet/go

go 1.26.0

toolchain go1.27.4

// Superseded by v0.1.0 on the same commit; the tag is protected and cached by the module proxy.
retract v0.1.0-draft.1

require (
	github.com/andybalholm/brotli v1.3.0
	github.com/klauspost/compress v1.20.4
	github.com/parquet-go/parquet-go v0.32.0
	github.com/pierrec/lz4/v4 v4.1.33
)

require (
	github.com/google/uuid v1.6.0 // indirect
	github.com/parquet-go/bitpack v1.1.0 // indirect
	github.com/parquet-go/jsonlite v1.0.0 // indirect
	github.com/twpayne/go-geom v1.6.1 // indirect
	golang.org/x/sys v0.50.0 // indirect
	google.golang.org/protobuf v1.36.12 // indirect
)
