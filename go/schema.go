// Copyright 2026 Varve Systems Ltd
// SPDX-License-Identifier: Apache-2.0

package seekparquet

import (
	"strconv"

	"github.com/parquet-go/parquet-go/format"
)

// Repetition is a Parquet field repetition.
type Repetition int8

// The repetitions.
const (
	Required Repetition = iota
	Optional
	Repeated
)

// String returns the Parquet name of the repetition, such as "OPTIONAL".
func (r Repetition) String() string {
	switch r {
	case Required:
		return "REQUIRED"
	case Optional:
		return "OPTIONAL"
	case Repeated:
		return "REPEATED"
	}
	return "Repetition(" + strconv.Itoa(int(r)) + ")"
}

// LogicalKind is the annotation a Logical type carries; LogicalNone, the
// zero LogicalKind, is none.
type LogicalKind uint8

// The logical annotations of Parquet's LogicalTypes.md.
const (
	LogicalNone LogicalKind = iota
	LogicalString
	LogicalMap
	LogicalList
	LogicalEnum
	LogicalDecimal
	LogicalDate
	LogicalTime
	LogicalTimestamp
	LogicalInt
	LogicalUnknown // Parquet's NULL annotation: every value is null
	LogicalJSON
	LogicalBSON
	LogicalUUID
	LogicalFloat16
	LogicalVariant
	LogicalGeometry
	LogicalGeography
)

// String returns the annotation's Parquet name, such as "TIMESTAMP".
func (k LogicalKind) String() string {
	if int(k) < len(logicalNames) {
		return logicalNames[k]
	}
	return "LogicalKind(" + strconv.Itoa(int(k)) + ")"
}

var logicalNames = [...]string{"NONE", "STRING", "MAP", "LIST", "ENUM", "DECIMAL", "DATE", "TIME", "TIMESTAMP",
	"INT", "NULL", "JSON", "BSON", "UUID", "FLOAT16", "VARIANT", "GEOMETRY", "GEOGRAPHY"}

// TimeUnit is the unit of a TIME or TIMESTAMP annotation.
type TimeUnit uint8

// The time units; TimeUnitNone is the zero TimeUnit, of every other
// annotation.
const (
	TimeUnitNone TimeUnit = iota
	Millis
	Micros
	Nanos
)

// String returns the unit's Parquet name, such as "MICROS", or "" for none.
func (u TimeUnit) String() string {
	switch u {
	case TimeUnitNone:
		return ""
	case Millis:
		return "MILLIS"
	case Micros:
		return "MICROS"
	case Nanos:
		return "NANOS"
	}
	return "TimeUnit(" + strconv.Itoa(int(u)) + ")"
}

// Logical is a column's logical annotation with its parameters. The zero
// Logical is no annotation. Only the fields of its Kind are set; the rest
// are zero. Logical values are comparable with ==.
type Logical struct {
	Kind LogicalKind
	// BitWidth (8, 16, 32 or 64) and Signed are an INT annotation's.
	BitWidth int8
	Signed   bool
	// Unit and AdjustedToUTC are a TIME or TIMESTAMP annotation's.
	Unit          TimeUnit
	AdjustedToUTC bool
	// Precision and Scale are a DECIMAL annotation's.
	Precision, Scale int32
	// SpecificationVersion is a VARIANT annotation's, 0 when not written.
	SpecificationVersion int8
	// CRS is a GEOMETRY or GEOGRAPHY annotation's coordinate reference
	// system, "" for the default OGC:CRS84.
	CRS string
	// Algorithm is a GEOGRAPHY annotation's edge interpolation algorithm, as
	// Parquet numbers them: 0 SPHERICAL (the default), 1 VINCENTY, 2 THOMAS,
	// 3 ANDOYER, 4 KARNEY.
	Algorithm int32
}

// String returns the annotation as Parquet tools print it, such as
// "TIMESTAMP(isAdjustedToUTC=true,unit=MICROS)", "INT(32,false)" or
// "DECIMAL(10,2)"; "NONE" for no annotation.
func (l Logical) String() string {
	if l.Kind == LogicalNone {
		return "NONE"
	}
	if lt := l.format(); lt != nil {
		return lt.String()
	}
	return l.Kind.String()
}

// logicalOf returns the Logical of a footer's logical type, the zero Logical
// for none.
func logicalOf(lt format.LogicalTypeValue) Logical {
	unit := func(u format.TimeUnit) TimeUnit {
		switch u.Value.(type) {
		case *format.MilliSeconds:
			return Millis
		case *format.MicroSeconds:
			return Micros
		case *format.NanoSeconds:
			return Nanos
		}
		return TimeUnitNone
	}
	switch t := lt.(type) {
	case *format.StringType:
		return Logical{Kind: LogicalString}
	case *format.MapType:
		return Logical{Kind: LogicalMap}
	case *format.ListType:
		return Logical{Kind: LogicalList}
	case *format.EnumType:
		return Logical{Kind: LogicalEnum}
	case *format.DecimalType:
		return Logical{Kind: LogicalDecimal, Precision: t.Precision, Scale: t.Scale}
	case *format.DateType:
		return Logical{Kind: LogicalDate}
	case *format.TimeType:
		return Logical{Kind: LogicalTime, Unit: unit(t.Unit), AdjustedToUTC: t.IsAdjustedToUTC}
	case *format.TimestampType:
		return Logical{Kind: LogicalTimestamp, Unit: unit(t.Unit), AdjustedToUTC: t.IsAdjustedToUTC}
	case *format.IntType:
		return Logical{Kind: LogicalInt, BitWidth: t.BitWidth, Signed: t.IsSigned}
	case *format.NullType:
		return Logical{Kind: LogicalUnknown}
	case *format.JsonType:
		return Logical{Kind: LogicalJSON}
	case *format.BsonType:
		return Logical{Kind: LogicalBSON}
	case *format.UUIDType:
		return Logical{Kind: LogicalUUID}
	case *format.Float16Type:
		return Logical{Kind: LogicalFloat16}
	case *format.VariantType:
		return Logical{Kind: LogicalVariant, SpecificationVersion: t.SpecificationVersion}
	case *format.GeometryType:
		return Logical{Kind: LogicalGeometry, CRS: t.CRS}
	case *format.GeographyType:
		return Logical{Kind: LogicalGeography, CRS: t.CRS, Algorithm: int32(t.Algorithm)}
	}
	return Logical{}
}

// format returns l as a footer's logical type, nil for none: the inverse of
// logicalOf.
func (l Logical) format() *format.LogicalType {
	unit := func() format.TimeUnit {
		switch l.Unit {
		case Millis:
			return format.TimeUnit{Value: &format.MilliSeconds{}}
		case Micros:
			return format.TimeUnit{Value: &format.MicroSeconds{}}
		case Nanos:
			return format.TimeUnit{Value: &format.NanoSeconds{}}
		}
		return format.TimeUnit{}
	}
	var v format.LogicalTypeValue
	switch l.Kind {
	case LogicalString:
		v = &format.StringType{}
	case LogicalMap:
		v = &format.MapType{}
	case LogicalList:
		v = &format.ListType{}
	case LogicalEnum:
		v = &format.EnumType{}
	case LogicalDecimal:
		v = &format.DecimalType{Precision: l.Precision, Scale: l.Scale}
	case LogicalDate:
		v = &format.DateType{}
	case LogicalTime:
		v = &format.TimeType{IsAdjustedToUTC: l.AdjustedToUTC, Unit: unit()}
	case LogicalTimestamp:
		v = &format.TimestampType{IsAdjustedToUTC: l.AdjustedToUTC, Unit: unit()}
	case LogicalInt:
		v = &format.IntType{BitWidth: l.BitWidth, IsSigned: l.Signed}
	case LogicalUnknown:
		v = &format.NullType{}
	case LogicalJSON:
		v = &format.JsonType{}
	case LogicalBSON:
		v = &format.BsonType{}
	case LogicalUUID:
		v = &format.UUIDType{}
	case LogicalFloat16:
		v = &format.Float16Type{}
	case LogicalVariant:
		v = &format.VariantType{SpecificationVersion: l.SpecificationVersion}
	case LogicalGeometry:
		v = &format.GeometryType{CRS: l.CRS}
	case LogicalGeography:
		v = &format.GeographyType{CRS: l.CRS, Algorithm: format.EdgeInterpolationAlgorithm(l.Algorithm)}
	default:
		return nil
	}
	return &format.LogicalType{Value: v}
}
