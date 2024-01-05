// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package expfmt contains tools for reading and writing Prometheus metrics.
package expfmt

import (
	"fmt"
	"strings"

	"github.com/prometheus/common/model"
)

// Format specifies the HTTP content type of the different wire protocols.
type Format string

// Constants to assemble the Content-Type values for the different wire protocols.
const (
	TextVersion_1_0_0        = "1.0.0"
	TextVersion_0_0_4        = "0.0.4"
	ProtoType                = `application/vnd.google.protobuf`
	ProtoProtocol            = `io.prometheus.client.MetricFamily`
	ProtoFmt                 = ProtoType + "; proto=" + ProtoProtocol + ";"
	UTF8Valid                = "utf8"
	OpenMetricsType          = `application/openmetrics-text`
	OpenMetricsVersion_2_0_0 = "2.0.0"
	OpenMetricsVersion_1_0_0 = "1.0.0"
	OpenMetricsVersion_0_0_1 = "0.0.1"

	// The Content-Type values for the different wire protocols. Do not do direct
	// to comparisons to these constants, instead use the comparison functions.
	FmtUnknown           Format = `<unknown>`
	FmtText_0_0_4        Format = `text/plain; version=` + TextVersion_0_0_4 + `; charset=utf-8`
	FmtText_1_0_0        Format = `text/plain; version=` + TextVersion_1_0_0 + `; charset=utf-8`
	FmtProtoDelim        Format = ProtoFmt + ` encoding=delimited`
	FmtProtoText         Format = ProtoFmt + ` encoding=text`
	FmtProtoCompact      Format = ProtoFmt + ` encoding=compact-text`
	FmtOpenMetrics_0_0_1 Format = OpenMetricsType + `; version=` + OpenMetricsVersion_0_0_1 + `; charset=utf-8`
	FmtOpenMetrics_1_0_0 Format = OpenMetricsType + `; version=` + OpenMetricsVersion_1_0_0 + `; charset=utf-8`
	FmtOpenMetrics_2_0_0 Format = OpenMetricsType + `; version=` + OpenMetricsVersion_2_0_0 + `; charset=utf-8`

	// UTF8 and Escaping Formats
	FmtUTF8Param         Format = `; validchars=utf8`
	FmtEscapeNone        Format = "none"
	FmtEscapeUnderscores Format = "underscores"
	FmtEscapeDots        Format = "dots"
	FmtEscapeValues      Format = "values"
)

const (
	hdrContentType = "Content-Type"
	hdrAccept      = "Accept"
)

type FormatType int

const (
	TypeUnknown = iota
	TypeProtoCompact
	TypeProtoDelim
	TypeProtoText
	TypeTextPlain
	TypeOpenMetrics
)

func (f Format) ContentType() FormatType {
	toks := strings.Split(string(f), ";")
	if len(toks) < 2 {
		return TypeUnknown
	}

	params := make(map[string]string)
	for i, t := range toks {
		if i == 0 {
			continue
		}
		args := strings.Split(t, "=")
		if len(args) != 2 {
			continue
		}
		params[strings.TrimSpace(args[0])] = strings.TrimSpace(args[1])
	}

	switch strings.TrimSpace(toks[0]) {
	case ProtoType:
		if params["proto"] != ProtoProtocol {
			return TypeUnknown
		}
		switch params["encoding"] {
		case "delimited":
			return TypeProtoDelim
		case "text":
			return TypeProtoText
		case "compact-text":
			return TypeProtoCompact
		default:
			return TypeUnknown
		}
	case OpenMetricsType:
		if params["charset"] != "utf-8" {
			return TypeUnknown
		}
		return TypeOpenMetrics
	case "text/plain":
		v, ok := params["version"]
		if !ok {
			return TypeTextPlain
		}
		if v == TextVersion_0_0_4 || v == TextVersion_1_0_0 {
			return TypeTextPlain
		}
		return TypeUnknown
	default:
		return TypeUnknown
	}
}

func EscapingSchemeToFormat(s model.EscapingScheme) Format {
	switch s {
	case model.NoEscaping:
		return FmtEscapeNone
	case model.UnderscoreEscaping:
		return FmtEscapeUnderscores
	case model.DotsEscaping:
		return FmtEscapeDots
	case model.ValueEncodingEscaping:
		return FmtEscapeValues
	default:
		panic(fmt.Sprintf("unknown escaping scheme %d", s))
	}
}

func (format Format) ToEscapingScheme() model.EscapingScheme {
	for _, p := range strings.Split(string(format), ";") {
		toks := strings.Split(p, "=")
		if len(toks) != 2 {
			continue
		}
		key, value := strings.TrimSpace(toks[0]), strings.TrimSpace(toks[1])
		// By definition, if utf8 is allowed then names are not escaped.
		if key == "validchars" && value == "utf8" {
			return model.NoEscaping
		}
		if key == "escaping" {
			switch f := Format(value); f {
			case FmtEscapeNone:
				return model.NoEscaping
			case FmtEscapeUnderscores:
				return model.UnderscoreEscaping
			case FmtEscapeDots:
				return model.DotsEscaping
			case FmtEscapeValues:
				return model.ValueEncodingEscaping
			default:
				panic("unknown format scheme " + f)
			}
		}
	}
	return model.DefaultNameEscapingScheme
}
