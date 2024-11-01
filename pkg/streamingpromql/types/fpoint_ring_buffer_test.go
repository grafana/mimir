package types

import (
	"testing"

	"github.com/prometheus/prometheus/promql"

	"github.com/grafana/mimir/pkg/streamingpromql/limiting"
)

func TestFPointRingBuffer_ChangesAtOrBefore(t *testing.T) {
	type fields struct {
		points     []promql.FPoint
		firstIndex int
		size       int
	}
	type args struct {
		maxT int64
	}
	tests := []struct {
		name          string
		fields        fields
		args          args
		wantChanges   float64
		wantHaveFloat bool
	}{
		{
			name: "Empty points",
			fields: fields{
				firstIndex: 0,
				size:       0,
				points:     []promql.FPoint{},
			},
			args: args{
				maxT: 0,
			},
			wantChanges:   0.0,
			wantHaveFloat: false,
		},
		{
			name: "Points that are not changing",
			fields: fields{
				firstIndex: 0,
				size:       5,
				points: []promql.FPoint{
					{T: 1, F: 1},
					{T: 3, F: 1},
					{T: 5, F: 1},
					{T: 7, F: 1},
					{T: 9, F: 1},
				},
			},
			args: args{
				maxT: 8,
			},
			wantChanges:   0.0,
			wantHaveFloat: true,
		},
		{
			name: "Changes on points that are changing at the time",
			fields: fields{
				firstIndex: 0,
				size:       5,
				points: []promql.FPoint{
					{T: 1, F: 1},
					{T: 3, F: 2},
					{T: 5, F: 3},
					{T: 7, F: 4},
					{T: 9, F: 99},
				},
			},
			args: args{
				maxT: 7,
			},
			wantChanges:   3.0,
			wantHaveFloat: true,
		},
		{
			name: "Changes on points that are changing before the time",
			fields: fields{
				firstIndex: 0,
				size:       5,
				points: []promql.FPoint{
					{T: 1, F: 1},
					{T: 3, F: 2},
					{T: 5, F: 3},
					{T: 7, F: 4},
					{T: 9, F: 99},
				},
			},
			args: args{
				maxT: 8,
			},
			wantChanges:   3.0,
			wantHaveFloat: true,
		},
		{
			name: "Changes on points that are changing at the time and its ring-buffer are wrapped around",
			fields: fields{
				firstIndex: 2,
				size:       5,
				points: []promql.FPoint{
					{T: 7, F: 4},
					{T: 9, F: 99},
					{T: 1, F: 1},
					{T: 3, F: 2},
					{T: 5, F: 3},
				},
			},
			args: args{
				maxT: 7,
			},
			wantChanges:   3.0,
			wantHaveFloat: true,
		},
		{
			name: "Changes on points that are changing before the time and its ring-buffer are wrapped around",
			fields: fields{
				firstIndex: 2,
				size:       5,
				points: []promql.FPoint{
					{T: 7, F: 4},
					{T: 9, F: 99},
					{T: 1, F: 1},
					{T: 3, F: 2},
					{T: 5, F: 3},
				},
			},
			args: args{
				maxT: 8,
			},
			wantChanges:   3.0,
			wantHaveFloat: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := &FPointRingBuffer{
				memoryConsumptionTracker: limiting.NewMemoryConsumptionTracker(0, nil),
				points:                   tt.fields.points,
				firstIndex:               tt.fields.firstIndex,
				size:                     tt.fields.size,
			}
			gotChanges, gotHaveFloat := b.ChangesAtOrBefore(tt.args.maxT)
			if gotChanges != tt.wantChanges {
				t.Errorf("ChangesAtOrBefore() gotChanges = %v, want %v", gotChanges, tt.wantChanges)
			}
			if gotHaveFloat != tt.wantHaveFloat {
				t.Errorf("ChangesAtOrBefore() gotHaveFloat = %v, want %v", gotHaveFloat, tt.wantHaveFloat)
			}
		})
	}
}
