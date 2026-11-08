// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

// Package testutil edits written datasets for this module's tests: their
// manifests and their parts' footers, to produce faults no writer makes.
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

// RewriteFooter applies change to the footer of the Parquet file at path
// and writes the file back with the new footer: for tests of footers no
// writer produces. The manifest's bytes then need SyncSizes.
func RewriteFooter(path string, change func(*format.FileMetaData) error) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	n := len(raw)
	footerLen := int(binary.LittleEndian.Uint32(raw[n-8 : n-4]))
	var protocol thrift.CompactProtocol
	var meta format.FileMetaData
	if err := thrift.Unmarshal(&protocol, raw[n-8-footerLen:n-8], &meta); err != nil {
		return fmt.Errorf("read footer: %w", err)
	}
	if err := change(&meta); err != nil {
		return err
	}
	footer, err := thrift.Marshal(&protocol, &meta)
	if err != nil {
		return err
	}
	body := append(raw[:n-8-footerLen:n-8-footerLen], footer...)
	body = binary.LittleEndian.AppendUint32(body, uint32(len(footer)))
	body = append(body, "PAR1"...)
	return os.WriteFile(path, body, 0o644)
}

// ReadFooter returns the footer of the Parquet file at path.
func ReadFooter(path string) (*format.FileMetaData, error) {
	var meta *format.FileMetaData
	err := rewriteNothing(path, func(m *format.FileMetaData) { meta = m })
	return meta, err
}

func rewriteNothing(path string, see func(*format.FileMetaData)) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	n := len(raw)
	footerLen := int(binary.LittleEndian.Uint32(raw[n-8 : n-4]))
	var meta format.FileMetaData
	if err := thrift.Unmarshal(new(thrift.CompactProtocol), raw[n-8-footerLen:n-8], &meta); err != nil {
		return err
	}
	see(&meta)
	return nil
}
