// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafkareceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkareceiver"

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkareceiver/internal/metadata"
)

// partitionDiagnostics is allocated only when explicitly enabled. Pause state
// follows receiver reasons; it does not inspect requests already in franz-go.
type partitionDiagnostics struct {
	mu       sync.Mutex
	builder  *metadata.TelemetryBuilder
	attrs    attribute.Set
	pauses   map[partitionPauseReason]time.Time
	resumed  time.Time
	active   int64
	canceled bool
}

func newPartitionDiagnostics(builder *metadata.TelemetryBuilder, attrs attribute.Set) *partitionDiagnostics {
	return &partitionDiagnostics{builder: builder, attrs: attrs, pauses: make(map[partitionPauseReason]time.Time)}
}

func (d *partitionDiagnostics) options(extra ...attribute.KeyValue) metric.MeasurementOption {
	attrs := append(d.attrs.ToSlice(), extra...)
	return metric.WithAttributeSet(attribute.NewSet(attrs...))
}

func (d *partitionDiagnostics) depth(n int) {
	d.builder.KafkaReceiverMailboxDepth.Record(context.Background(), int64(n), d.options())
}

func (d *partitionDiagnostics) queueDuration(start time.Time, outcome string) {
	d.builder.KafkaReceiverMailboxQueueDuration.Record(context.Background(), time.Since(start).Seconds(), d.options(attribute.String("wait_outcome", outcome)))
}

func pauseReasonName(reason partitionPauseReason) string {
	switch reason {
	case partitionPauseBackpressure:
		return "backpressure"
	case partitionPauseRewind:
		return "rewind"
	default:
		return "terminal"
	}
}

// addPauseLocked and clearPauseLocked run under the same lock as changes to
// pc.pauseReasons, preserving ordering between worker and poll-loop observers.
func (d *partitionDiagnostics) addPauseLocked(reason partitionPauseReason) {
	if d.canceled {
		return
	}
	if _, exists := d.pauses[reason]; exists {
		return
	}
	if !d.resumed.IsZero() {
		d.recordResumeLocked("discarded")
	}
	d.pauses[reason] = time.Now()
	d.builder.KafkaReceiverPartitionPaused.Record(context.Background(), 1, d.options())
}

func (d *partitionDiagnostics) clearPauseLocked(reasons partitionPauseReason, remaining uint32) {
	if d.canceled {
		return
	}
	changed := false
	for reason, start := range d.pauses {
		if reason&reasons != 0 {
			d.recordPause(reason, start, "success")
			delete(d.pauses, reason)
			changed = true
		}
	}
	if changed && remaining == 0 {
		d.builder.KafkaReceiverPartitionPaused.Record(context.Background(), 0, d.options())
		d.resumed = time.Now()
	}
}

func (d *partitionDiagnostics) recordPause(reason partitionPauseReason, start time.Time, outcome string) {
	d.builder.KafkaReceiverPartitionPauseDuration.Record(context.Background(), time.Since(start).Seconds(), d.options(attribute.String("pause_reason", pauseReasonName(reason)), attribute.String("wait_outcome", outcome)))
}

func (d *partitionDiagnostics) recordResumeLocked(outcome string) {
	d.builder.KafkaReceiverResumeToDataDuration.Record(context.Background(), time.Since(d.resumed).Seconds(), d.options(attribute.String("wait_outcome", outcome)))
	d.resumed = time.Time{}
}

func (d *partitionDiagnostics) dataReceived() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.canceled && !d.resumed.IsZero() {
		d.recordResumeLocked("success")
	}
}

func (d *partitionDiagnostics) processing(delta int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.active += delta
	if !d.canceled {
		d.builder.KafkaReceiverProcessingRecords.Record(context.Background(), d.active, d.options())
	}
}

func (d *partitionDiagnostics) cancel() {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.canceled {
		return
	}
	d.canceled = true
	for reason, start := range d.pauses {
		d.recordPause(reason, start, "canceled")
	}
	clear(d.pauses)
	if !d.resumed.IsZero() {
		d.recordResumeLocked("canceled")
	}
	d.builder.KafkaReceiverPartitionPaused.Record(context.Background(), 0, d.options())
	// Cancel removes this assignment; a timed-out old worker must not overwrite
	// the new owner's gauge for the same topic-partition when it eventually exits.
	d.builder.KafkaReceiverProcessingRecords.Record(context.Background(), 0, d.options())
}

func recordProcessingDuration(ctx context.Context, builder *metadata.TelemetryBuilder, attrs attribute.Set, phase string, started time.Time, err error) {
	outcome := "success"
	if err != nil {
		outcome = "failure"
	}
	if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		outcome = "canceled"
	}
	opts := metric.WithAttributeSet(attribute.NewSet(append(attrs.ToSlice(), attribute.String("phase", phase), attribute.String("wait_outcome", outcome))...))
	builder.KafkaReceiverRecordProcessingDuration.Record(context.WithoutCancel(ctx), time.Since(started).Seconds(), opts)
}
