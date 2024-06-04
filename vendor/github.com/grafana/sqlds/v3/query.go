package sqlds

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/grafana/dataplane/sdata/timeseries"
	"github.com/grafana/grafana-plugin-sdk-go/backend"
	"github.com/grafana/grafana-plugin-sdk-go/data"
	"github.com/grafana/grafana-plugin-sdk-go/data/sqlutil"
)

// FormatQueryOption defines how the user has chosen to represent the data
// Deprecated: use sqlutil.FormatQueryOption directly instead
type FormatQueryOption = sqlutil.FormatQueryOption

// Deprecated: use the values in sqlutil directly instead
const (
	// FormatOptionTimeSeries formats the query results as a timeseries using "LongToWide"
	FormatOptionTimeSeries = sqlutil.FormatOptionTimeSeries
	// FormatOptionTable formats the query results as a table using "LongToWide"
	FormatOptionTable = sqlutil.FormatOptionTable
	// FormatOptionLogs sets the preferred visualization to logs
	FormatOptionLogs = sqlutil.FormatOptionLogs
	// FormatOptionsTrace sets the preferred visualization to trace
	FormatOptionTrace = sqlutil.FormatOptionTrace
	// FormatOptionMulti formats the query results as a timeseries using "LongToMulti"
	FormatOptionMulti = sqlutil.FormatOptionMulti
)

// Deprecated: use sqlutil.Query directly instead
type Query = sqlutil.Query

// GetQuery wraps sqlutil's GetQuery to add headers if needed
func GetQuery(query backend.DataQuery, headers http.Header, setHeaders bool) (*Query, error) {
	model, err := sqlutil.GetQuery(query)

	if err != nil {
		return nil, PluginError(err)
	}

	if setHeaders {
		applyHeaders(model, headers)
	}

	return model, nil
}

// QueryDB sends the query to the connection and converts the rows to a dataframe.
func QueryDB(ctx context.Context, db Connection, converters []sqlutil.Converter, fillMode *data.FillMissing, query *Query, args ...interface{}) (data.Frames, error) {
	// Query the rows from the database
	rows, err := db.QueryContext(ctx, query.RawSQL, args...)
	if err != nil {
		errType := ErrorQuery
		if errors.Is(err, context.Canceled) {
			errType = context.Canceled
		}
		err := DownstreamError(fmt.Errorf("%w: %s", errType, err.Error()))
		return sqlutil.ErrorFrameFromQuery(query), err
	}

	// Check for an error response
	if err := rows.Err(); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// Should we even response with an error here?
			// The panel will simply show "no data"
			err := DownstreamError(fmt.Errorf("%s: %w", "No results from query", err))
			return sqlutil.ErrorFrameFromQuery(query), err
		}
		err := DownstreamError(fmt.Errorf("%s: %w", "Error response from database", err))
		return sqlutil.ErrorFrameFromQuery(query), err
	}

	defer func() {
		if err := rows.Close(); err != nil {
			backend.Logger.Error(err.Error())
		}
	}()

	// Convert the response to frames
	res, err := getFrames(rows, -1, converters, fillMode, query)
	if err != nil {
		err := PluginError(fmt.Errorf("%w: %s", err, "Could not process SQL results"))
		return sqlutil.ErrorFrameFromQuery(query), err
	}

	return res, nil
}

func getFrames(rows *sql.Rows, limit int64, converters []sqlutil.Converter, fillMode *data.FillMissing, query *Query) (data.Frames, error) {
	frame, err := sqlutil.FrameFromRows(rows, limit, converters...)
	if err != nil {
		return nil, err
	}
	frame.Name = query.RefID
	if frame.Meta == nil {
		frame.Meta = &data.FrameMeta{}
	}

	count, err := frame.RowLen()
	if err != nil {
		return nil, err
	}
	if count == 0 {
		return nil, ErrorNoResults
	}

	frame.Meta.ExecutedQueryString = query.RawSQL
	frame.Meta.PreferredVisualization = data.VisTypeGraph

	switch query.Format {
	case FormatOptionMulti:
		if frame.TimeSeriesSchema().Type == data.TimeSeriesTypeLong {

			err = fixFrameForLongToMulti(frame)
			if err != nil {
				return nil, err
			}

			frames, err := timeseries.LongToMulti(&timeseries.LongFrame{frame})
			if err != nil {
				return nil, err
			}
			return frames.Frames(), nil
		}
	case FormatOptionTable:
		frame.Meta.PreferredVisualization = data.VisTypeTable
	case FormatOptionLogs:
		frame.Meta.PreferredVisualization = data.VisTypeLogs
	case FormatOptionTrace:
		frame.Meta.PreferredVisualization = data.VisTypeTrace
	// Format as timeSeries
	default:
		if frame.TimeSeriesSchema().Type == data.TimeSeriesTypeLong {
			frame, err = data.LongToWide(frame, fillMode)
			if err != nil {
				return nil, err
			}
		}
	}
	return data.Frames{frame}, nil
}

// fixFrameForLongToMulti edits the passed in frame so that it's first time field isn't nullable and has the correct meta
func fixFrameForLongToMulti(frame *data.Frame) error {
	if frame == nil {
		return fmt.Errorf("can not convert to wide series, input is nil")
	}

	timeFields := frame.TypeIndices(data.FieldTypeTime, data.FieldTypeNullableTime)
	if len(timeFields) == 0 {
		return fmt.Errorf("can not convert to wide series, input is missing a time field")
	}

	// the timeseries package expects the first time field in the frame to be non-nullable and ignores the rest
	timeField := frame.Fields[timeFields[0]]
	if timeField.Type() == data.FieldTypeNullableTime {
		newValues := []time.Time{}
		for i := 0; i < timeField.Len(); i++ {
			val, ok := timeField.ConcreteAt(i)
			if !ok {
				return fmt.Errorf("can not convert to wide series, input has null time values")
			}
			newValues = append(newValues, val.(time.Time))
		}
		newField := data.NewField(timeField.Name, timeField.Labels, newValues)
		newField.Config = timeField.Config
		frame.Fields[timeFields[0]] = newField

		// LongToMulti requires the meta to be set for the frame
		if frame.Meta == nil {
			frame.Meta = &data.FrameMeta{}
		}
		frame.Meta.Type = data.FrameTypeTimeSeriesLong
		frame.Meta.TypeVersion = data.FrameTypeVersion{0, 1}
	}
	return nil
}

func applyHeaders(query *Query, headers http.Header) *Query {
	var args map[string]interface{}
	if query.ConnectionArgs == nil {
		query.ConnectionArgs = []byte("{}")
	}
	err := json.Unmarshal(query.ConnectionArgs, &args)
	if err != nil {
		backend.Logger.Warn(fmt.Sprintf("Failed to apply headers: %s", err.Error()))
		return query
	}
	args[HeaderKey] = headers
	raw, err := json.Marshal(args)
	if err != nil {
		backend.Logger.Warn(fmt.Sprintf("Failed to apply headers: %s", err.Error()))
		return query
	}

	query.ConnectionArgs = raw

	return query
}
