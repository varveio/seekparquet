// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"slices"

	"github.com/parquet-go/parquet-go/deprecated"
	"github.com/parquet-go/parquet-go/encoding/thrift"
	"github.com/parquet-go/parquet-go/format"
)

// footerMetadata follows FileMetaData, but preserves optional KeyValue.value
// presence and LogicalType wire presence. parquet-go incorrectly requires
// KeyValue.value and discards unknown unions. Both decode passes
// must accept valueless non-stamp metadata (STAMP-2).
type footerMetadata struct {
	Version int32 `thrift:"1,required"`

	Schema thrift.Slice[footerSchemaElement] `thrift:"2,required"`

	NumRows int64 `thrift:"3,required"`

	RowGroups thrift.Slice[format.RowGroup] `thrift:"4,required"`

	KeyValueMetadata thrift.Slice[footerKeyValue] `thrift:"5,optional"`

	CreatedBy string `thrift:"6,optional"`

	ColumnOrders thrift.Slice[format.ColumnOrder] `thrift:"7,optional"`

	EncryptionAlgorithm format.EncryptionAlgorithm `thrift:"8,optional"`

	FooterSigningKeyMetadata []byte `thrift:"9,optional"`
}

type footerKeyValue struct {
	Key   string              `thrift:"1,required"`
	Value thrift.Null[string] `thrift:"2,optional"`
}

func (w *footerMetadata) metadata() (format.FileMetaData, bool) {
	m := format.FileMetaData{
		Version:                  w.Version,
		NumRows:                  w.NumRows,
		RowGroups:                w.RowGroups,
		CreatedBy:                w.CreatedBy,
		ColumnOrders:             w.ColumnOrders,
		EncryptionAlgorithm:      w.EncryptionAlgorithm,
		FooterSigningKeyMetadata: w.FooterSigningKeyMetadata,
	}
	for _, se := range w.Schema {
		logical := format.LogicalType{Value: se.LogicalType.V.Value}
		if se.LogicalType.Valid && logical.Value == nil {
			logical.Value = &unrecognizedLogicalType{}
		}
		m.Schema = append(m.Schema, format.SchemaElement{
			Type: se.Type, TypeLength: se.TypeLength, RepetitionType: se.RepetitionType, Name: se.Name, NumChildren: se.NumChildren,
			ConvertedType: se.ConvertedType, Scale: se.Scale, Precision: se.Precision, FieldID: se.FieldID, LogicalType: logical,
		})
	}

	normalized := false
	for _, kv := range w.KeyValueMetadata {
		normalized = normalized || !kv.Value.Valid
		// No listed stamp can have an empty value. Preserve valueless stamp
		// occurrences as empty strings so checkStamps/readStamps count them
		// for duplicates and forbidden stamps and otherwise report mismatch.
		// Drop ignored valueless entries before parquet-go's second decode.
		if kv.Value.Valid || slices.Contains(stampKeys, kv.Key) {
			m.KeyValueMetadata = append(m.KeyValueMetadata, format.KeyValue{Key: kv.Key, Value: kv.Value.V})
		}
	}
	return m, normalized
}

// Preserve wire presence: an unknown LogicalType member is not an absent
// annotation. Value columns can use converted fallback; KEY-1 still refuses it.
type footerSchemaElement struct {
	Type           thrift.Null[format.Type]                `thrift:"1,optional"`
	TypeLength     thrift.Null[int32]                      `thrift:"2,optional"`
	RepetitionType thrift.Null[format.FieldRepetitionType] `thrift:"3,optional"`
	Name           string                                  `thrift:"4,required"`
	NumChildren    thrift.Null[int32]                      `thrift:"5,optional"`
	ConvertedType  thrift.Null[deprecated.ConvertedType]   `thrift:"6,optional"`
	Scale          thrift.Null[int32]                      `thrift:"7,optional"`
	Precision      thrift.Null[int32]                      `thrift:"8,optional"`
	FieldID        int32                                   `thrift:"9,optional"`
	LogicalType    thrift.Null[footerLogicalType]          `thrift:"10,optional"`
}

// parquet-go lacks members 19 and 20 from the pinned parquet-format revision.
// Recognize their wire IDs so CHECK-1 rejects these group annotations on leaves.
// Group semantics and parameters are not interpreted by this reader.
type footerLogicalType struct {
	Value format.LogicalTypeValue
}

func (*footerLogicalType) UnionMembers() []thrift.UnionMember {
	return slices.Concat((&format.LogicalType{}).UnionMembers(), []thrift.UnionMember{(*fileLogicalType)(nil), (*finiteVectorLogicalType)(nil)})
}

type fileLogicalType struct{}

func (*fileLogicalType) FieldID() int16 { return 19 }
func (*fileLogicalType) String() string { return "FILE" }

type finiteVectorLogicalType struct{}

func (*finiteVectorLogicalType) FieldID() int16 { return 20 }
func (*finiteVectorLogicalType) String() string { return "FINITE_VECTOR" }

type unrecognizedLogicalType struct{}

func (*unrecognizedLogicalType) FieldID() int16 { return 32767 }
func (*unrecognizedLogicalType) String() string { return "unrecognized logical type" }
