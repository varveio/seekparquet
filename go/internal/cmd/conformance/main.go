// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Command conformance is the Go implementation's adapter for the
// SeekParquet conformance harness (conformance/protocol.md): it reads one
// JSON request per line on stdin and writes one JSON response per line on
// stdout, in order, until stdin closes. Diagnostics go to stderr.
package main

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"

	"github.com/parquet-go/parquet-go"

	"github.com/varveio/seekparquet/go"
	"github.com/varveio/seekparquet/go/internal/manifest"
)

// version is the implementation version the adapter reports.
const version = "v0.0.0-dev"

func main() {
	a := &adapter{open: map[string]*seekparquet.Dataset{}}
	defer a.closeAll()
	if err := a.serve(context.Background(), os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "conformance:", err)
		os.Exit(1)
	}
}

type adapter struct {
	open map[string]*seekparquet.Dataset
}

func (a *adapter) closeAll() {
	for path, d := range a.open {
		_ = d.Close()
		delete(a.open, path)
	}
}

// serve answers every request line of in on out.
func (a *adapter) serve(ctx context.Context, in io.Reader, out io.Writer) error {
	scanner := bufio.NewScanner(in)
	scanner.Buffer(make([]byte, 64<<10), 64<<20)
	w := bufio.NewWriter(out)
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		resp := a.handle(ctx, line)
		raw, err := json.Marshal(resp)
		if err != nil {
			raw, _ = json.Marshal(map[string]any{"id": resp["id"], "status": "error", "message": err.Error()})
		}
		if _, err := w.Write(append(raw, '\n')); err != nil {
			return err
		}
		if err := w.Flush(); err != nil {
			return err
		}
	}
	return scanner.Err()
}

type response = map[string]any

// handle answers one request line.
func (a *adapter) handle(ctx context.Context, line []byte) response {
	var req map[string]json.RawMessage
	if err := json.Unmarshal(line, &req); err != nil || req == nil {
		return response{"id": nil, "status": "invalid_request", "message": "the request is not a JSON object"}
	}
	var id any
	if raw, ok := req["id"]; ok {
		_ = json.Unmarshal(raw, &id)
	}
	resp := a.dispatch(ctx, req)
	resp["id"] = id
	return resp
}

func (a *adapter) dispatch(ctx context.Context, req map[string]json.RawMessage) response {
	var op string
	if err := field(req, "op", &op); err != nil {
		return invalid(err)
	}
	switch op {
	case "hello":
		return response{
			"status":         "ok",
			"implementation": "seekparquet-go",
			"version":        version,
			"supports":       supports(),
			"targets":        []string{"reader", "validator-structural", "validator-profile", "validator-full", "validator-digest"},
			"capabilities": []string{"open", "range", "list", "projection", "verify",
				"validate:structural", "validate:profile", "validate:full", "validate:digest"},
		}
	case "open":
		path, err := datasetPath(req)
		if err != nil {
			return invalid(err)
		}
		if d := a.open[path]; d != nil {
			_ = d.Close()
			delete(a.open, path)
		}
		if _, err := a.dataset(ctx, path); err != nil {
			return failure(err)
		}
		return response{"status": "ok"}
	case "verify":
		path, err := datasetPath(req)
		if err != nil {
			return invalid(err)
		}
		d, err := a.dataset(ctx, path)
		if err == nil {
			err = d.Verify(ctx)
		}
		if err != nil {
			return failure(err)
		}
		return response{"status": "ok"}
	case "range":
		return a.rangeOp(ctx, req)
	case "list":
		return a.listOp(ctx, req)
	case "validate":
		return validateOp(ctx, req)
	default:
		return response{"status": "invalid_request", "message": fmt.Sprintf("unknown op %q", op)}
	}
}

// validateOp validates a dataset at the requested level and reports each
// level attempted (protocol.md, validate).
func validateOp(ctx context.Context, req map[string]json.RawMessage) response {
	path, err := datasetPath(req)
	if err != nil {
		return invalid(err)
	}
	var level string
	if err := field(req, "level", &level); err != nil {
		return invalid(err)
	}
	switch seekparquet.Level(level) {
	case seekparquet.LevelStructural, seekparquet.LevelProfile, seekparquet.LevelFull, seekparquet.LevelDigest:
	default:
		return invalid(fmt.Errorf("unknown level %q", level))
	}
	report, err := seekparquet.ValidateDir(ctx, path, seekparquet.ValidateOptions{Level: seekparquet.Level(level)})
	if err != nil {
		fmt.Fprintln(os.Stderr, "conformance:", err)
		return response{"status": "error", "message": err.Error()}
	}
	levels := map[string]any{}
	for _, lr := range report.Levels {
		out := map[string]any{"result": string(lr.Result)}
		switch lr.Result {
		case seekparquet.Failed:
			out["code"] = string(lr.Failure.Code)
			out["message"] = lr.Failure.Error()
		case seekparquet.Incomplete:
			out["reason"] = lr.Reason
		}
		if lr.Level == seekparquet.LevelDigest {
			out["checked"] = lr.Checked.Digests
		}
		levels[string(lr.Level)] = out
	}
	return response{"status": "ok", "levels": levels}
}

func supports() []map[string]int64 {
	var out []map[string]int64
	for _, v := range seekparquet.SupportedVersions() {
		out = append(out, map[string]int64{"format_version": v.FormatVersion, "draft_revision": v.DraftRevision})
	}
	return out
}

// dataset returns the open dataset at path, opening it on first use.
func (a *adapter) dataset(ctx context.Context, path string) (*seekparquet.Dataset, error) {
	if d := a.open[path]; d != nil {
		return d, nil
	}
	d, err := seekparquet.OpenDir(ctx, path, seekparquet.Options{})
	if err != nil {
		return nil, err
	}
	a.open[path] = d
	return d, nil
}

func (a *adapter) rangeOp(ctx context.Context, req map[string]json.RawMessage) response {
	path, err := datasetPath(req)
	if err != nil {
		return invalid(err)
	}
	var rng seekparquet.Range
	if rng.Prefix, err = bytesField(req, "prefix_base64"); err != nil {
		return invalid(err)
	}
	if rng.Start, err = bytesField(req, "start_base64"); err != nil {
		return invalid(err)
	}
	if rng.End, err = bytesField(req, "end_base64"); err != nil {
		return invalid(err)
	}
	if err := optionalField(req, "start_exclusive", &rng.StartExclusive); err != nil {
		return invalid(err)
	}
	if rng.Cursor, err = cursorField(req); err != nil {
		return invalid(err)
	}
	if rng.Limit, err = limitField(req); err != nil {
		return invalid(err)
	}
	if err := optionalField(req, "columns", &rng.Columns); err != nil {
		return invalid(err)
	}
	d, err := a.dataset(ctx, path)
	if err != nil {
		return failure(err)
	}
	rows, err := d.Range(ctx, rng)
	if err != nil {
		return failure(err)
	}
	entries := make([]any, 0, len(rows))
	for i := range rows {
		e, err := rowEntry(&rows[i])
		if err != nil {
			return failure(err)
		}
		entries = append(entries, e)
	}
	return response{"status": "ok", "entries": entries}
}

func (a *adapter) listOp(ctx context.Context, req map[string]json.RawMessage) response {
	path, err := datasetPath(req)
	if err != nil {
		return invalid(err)
	}
	var list seekparquet.List
	if list.Prefix, err = bytesField(req, "prefix_base64"); err != nil {
		return invalid(err)
	}
	delim, err := bytesField(req, "delimiter_base64")
	if err != nil {
		return invalid(err)
	}
	if len(delim) != 1 {
		return invalid(errors.New("the delimiter is not exactly one byte"))
	}
	list.Delimiter = delim[0]
	if list.End, err = bytesField(req, "end_base64"); err != nil {
		return invalid(err)
	}
	if list.Cursor, err = cursorField(req); err != nil {
		return invalid(err)
	}
	if list.Limit, err = limitField(req); err != nil {
		return invalid(err)
	}
	if err := optionalField(req, "columns", &list.Columns); err != nil {
		return invalid(err)
	}
	d, err := a.dataset(ctx, path)
	if err != nil {
		return failure(err)
	}
	got, err := d.List(ctx, list)
	if err != nil {
		return failure(err)
	}
	entries := make([]any, 0, len(got))
	for _, e := range got {
		if e.IsPrefix() {
			entries = append(entries, map[string]any{"kind": "prefix", "key_base64": base64.StdEncoding.EncodeToString(e.CommonPrefix)})
			continue
		}
		row, err := rowEntry(e.Row)
		if err != nil {
			return failure(err)
		}
		entries = append(entries, row)
	}
	return response{"status": "ok", "entries": entries}
}

// rowEntry encodes a row; a value the protocol cannot express (INT96) is
// unsupported.
func rowEntry(row *seekparquet.Row) (map[string]any, error) {
	values := make([]any, 0, len(row.Values))
	for i, v := range row.Values {
		enc, err := encodeValue(v)
		if err != nil {
			return nil, fmt.Errorf("column %s: %w", row.Columns[i], err)
		}
		values = append(values, enc)
	}
	return map[string]any{"kind": "row", "key_base64": base64.StdEncoding.EncodeToString(row.Key), "offset": row.Offset, "values": values}, nil
}

// cursorField decodes the optional tagged cursor "after": exactly
// {"key_base64", "offset"} after a row, or {"prefix_base64"} after a
// common prefix. Whether the operation accepts its kind is the library's
// check.
func cursorField(req map[string]json.RawMessage) (*seekparquet.Cursor, error) {
	var tag map[string]json.RawMessage
	if err := optionalField(req, "after", &tag); err != nil || tag == nil {
		return nil, err
	}
	_, hasKey := tag["key_base64"]
	_, hasOffset := tag["offset"]
	_, hasPrefix := tag["prefix_base64"]
	switch {
	case hasKey && hasOffset && len(tag) == 2:
		key, err := bytesField(tag, "key_base64")
		if err != nil || key == nil {
			return nil, errors.New("after: key_base64 is missing or not canonical base64")
		}
		offset, err := manifest.Integer(tag["offset"])
		if err != nil {
			return nil, fmt.Errorf("after: offset: %w", err)
		}
		return &seekparquet.Cursor{Key: key, Offset: offset}, nil
	case hasPrefix && len(tag) == 1:
		prefix, err := bytesField(tag, "prefix_base64")
		if err != nil || prefix == nil {
			return nil, errors.New("after: prefix_base64 is missing or not canonical base64")
		}
		return &seekparquet.Cursor{Prefix: prefix}, nil
	}
	return nil, errors.New("after is not a row or prefix cursor")
}

// errInt96 reports a value the protocol has no encoding for.
var errInt96 = &seekparquet.UnsupportedError{Code: seekparquet.UnsupportedProjection, Detail: "INT96 values have no protocol encoding"}

// encodeValue encodes a projected value as its physical Parquet value.
func encodeValue(v seekparquet.Value) (map[string]any, error) {
	switch {
	case v.Absent:
		return map[string]any{"absent": true}, nil
	case v.IsNull():
		return map[string]any{"null": true}, nil
	}
	switch v.Kind() {
	case parquet.Boolean:
		return map[string]any{"bool": v.Boolean()}, nil
	case parquet.Int32:
		return map[string]any{"int": strconv.FormatInt(int64(v.Int32()), 10)}, nil
	case parquet.Int64:
		return map[string]any{"int": strconv.FormatInt(v.Int64(), 10)}, nil
	case parquet.Float:
		return map[string]any{"float32": fmt.Sprintf("%08x", math.Float32bits(v.Float()))}, nil
	case parquet.Double:
		return map[string]any{"float64": fmt.Sprintf("%016x", math.Float64bits(v.Double()))}, nil
	case parquet.ByteArray, parquet.FixedLenByteArray:
		return map[string]any{"bytes_base64": base64.StdEncoding.EncodeToString(v.ByteArray())}, nil
	}
	return nil, errInt96
}

// failure classifies an error from the library.
func failure(err error) response {
	var unsupported *seekparquet.UnsupportedError
	switch {
	case seekparquet.CodeOf(err) != "":
		return response{"status": "refused", "code": string(seekparquet.CodeOf(err)), "message": err.Error()}
	case errors.Is(err, seekparquet.ErrInvalidRequest):
		return response{"status": "invalid_request", "message": err.Error()}
	case errors.As(err, &unsupported):
		return response{"status": "unsupported", "code": unsupported.Code, "message": err.Error()}
	}
	fmt.Fprintln(os.Stderr, "conformance:", err)
	return response{"status": "error", "message": err.Error()}
}

func invalid(err error) response {
	return response{"status": "invalid_request", "message": err.Error()}
}

// field decodes a required member.
func field(req map[string]json.RawMessage, name string, into any) error {
	raw, ok := req[name]
	if !ok || string(raw) == "null" {
		return fmt.Errorf("%s is missing", name)
	}
	if err := json.Unmarshal(raw, into); err != nil {
		return fmt.Errorf("%s is ill-typed", name)
	}
	return nil
}

// optionalField decodes a member that may be absent or null.
func optionalField(req map[string]json.RawMessage, name string, into any) error {
	raw, ok := req[name]
	if !ok || string(raw) == "null" {
		return nil
	}
	if err := json.Unmarshal(raw, into); err != nil {
		return fmt.Errorf("%s is ill-typed", name)
	}
	return nil
}

func datasetPath(req map[string]json.RawMessage) (string, error) {
	var path string
	err := field(req, "dataset", &path)
	return path, err
}

// bytesField decodes an optional base64 member: nil when absent or null,
// and an empty non-nil slice for "".
func bytesField(req map[string]json.RawMessage, name string) ([]byte, error) {
	var s *string
	if err := optionalField(req, name, &s); err != nil {
		return nil, err
	}
	if s == nil {
		return nil, nil
	}
	b, err := base64.StdEncoding.Strict().DecodeString(*s)
	if err != nil || base64.StdEncoding.EncodeToString(b) != *s {
		return nil, fmt.Errorf("%s is not canonical standard base64", name)
	}
	if b == nil {
		b = []byte{}
	}
	return b, nil
}

// limitField decodes the limit, which must be a JSON integer; whether it
// is positive is the library's check (QUERY-1).
func limitField(req map[string]json.RawMessage) (int, error) {
	raw, ok := req["limit"]
	if !ok {
		return 0, errors.New("limit is missing")
	}
	n, err := strconv.ParseInt(string(raw), 10, 64)
	if errors.Is(err, strconv.ErrRange) {
		// An integer beyond 64 bits: its sign is all that matters.
		n, err = math.MaxInt64, nil
		if raw[0] == '-' {
			n = math.MinInt64
		}
	}
	if err != nil {
		return 0, errors.New("limit is not an integer")
	}
	if n > math.MaxInt {
		n = math.MaxInt
	}
	if n < math.MinInt {
		n = math.MinInt
	}
	return int(n), nil
}
