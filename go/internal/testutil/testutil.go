// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package testutil writes small SeekParquet datasets for this module's
// tests (Write), and edits them, their manifests and their parts' footers,
// to produce faults no writer makes.
package testutil

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

func writeManifest(dir string, m map[string]any) error {
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "manifest.json"), raw, 0o644)
}

// EditManifest decodes dir's manifest, applies edit and writes it back.
func EditManifest(dir string, edit func(m map[string]any)) error {
	raw, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil {
		return err
	}
	edit(m)
	return writeManifest(dir, m)
}

// ManifestPart returns part i of a decoded manifest, for EditManifest.
func ManifestPart(m map[string]any, i int) map[string]any {
	return m["parts"].([]any)[i].(map[string]any)
}

// SyncSizes sets every part's bytes in dir's manifest to its file's size,
// after a test rewrote a part.
func SyncSizes(dir string) error {
	var err error
	editErr := EditManifest(dir, func(m map[string]any) {
		for _, p := range m["parts"].([]any) {
			entry := p.(map[string]any)
			info, statErr := os.Stat(filepath.Join(dir, filepath.FromSlash(entry["path"].(string))))
			if statErr != nil {
				err = statErr
				return
			}
			entry["bytes"] = info.Size()
		}
	})
	return errors.Join(editErr, err)
}

// splitFooter returns the body of a Parquet file and its decoded footer.
func splitFooter(raw []byte) ([]byte, *format.FileMetaData, error) {
	n := len(raw)
	footerLen := int(binary.LittleEndian.Uint32(raw[n-8 : n-4]))
	var meta format.FileMetaData
	if err := thrift.Unmarshal(new(thrift.CompactProtocol), raw[n-8-footerLen:n-8], &meta); err != nil {
		return nil, nil, fmt.Errorf("read footer: %w", err)
	}
	return raw[: n-8-footerLen : n-8-footerLen], &meta, nil
}

// RewriteFooter applies change to the footer of the Parquet file at path
// and writes the file back with the new footer: for tests of footers no
// writer produces. The manifest's bytes then need SyncSizes.
func RewriteFooter(path string, change func(*format.FileMetaData) error) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	body, meta, err := splitFooter(raw)
	if err != nil {
		return err
	}
	if err := change(meta); err != nil {
		return err
	}
	footer, err := thrift.Marshal(new(thrift.CompactProtocol), meta)
	if err != nil {
		return err
	}
	body = append(body, footer...)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(footer)))
	body = append(body, "PAR1"...)
	return os.WriteFile(path, body, 0o644)
}

// ReadFooter returns the footer of the Parquet file at path.
func ReadFooter(path string) (*format.FileMetaData, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	_, meta, err := splitFooter(raw)
	return meta, err
}
