// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package kafkareceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkareceiver"

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/testdata"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/kafkareceiver/internal/metadata"
)

func diagnosticHistogram(t *testing.T, tel *componenttest.Telemetry, name string) metricdata.Histogram[float64] {
	t.Helper()
	m, err := tel.GetMetric(name)
	require.NoError(t, err)
	h, ok := m.Data.(metricdata.Histogram[float64])
	require.True(t, ok)
	return h
}

func diagnosticPartition(t *testing.T) (*pc, *componenttest.Telemetry) {
	t.Helper()
	tel := componenttest.NewTelemetry()
	tb, err := metadata.NewTelemetryBuilder(tel.NewTelemetrySettings())
	require.NoError(t, err)
	t.Cleanup(func() { tb.Shutdown(); require.NoError(t, tel.Shutdown(context.Background())) })
	p := &pc{attrs: attribute.NewSet(attribute.String("topic", "test"), attribute.Int64("partition", 0))}
	p.ctx, p.cancel = context.WithCancelCause(t.Context())
	p.diagnostics = newPartitionDiagnostics(tb, p.attrs)
	return p, tel
}

func TestPartitionDiagnosticsOverlappingPauses(t *testing.T) {
	p, tel := diagnosticPartition(t)
	p.addPauseReason(partitionPauseBackpressure)
	p.addPauseReason(partitionPauseBackpressure) // Repeated pause must not reset its start.
	p.addPauseReason(partitionPauseRewind)
	require.False(t, p.clearPauseReasons(partitionPauseBackpressure))
	require.True(t, p.diagnostics.resumed.IsZero(), "still paused for rewind")
	require.True(t, p.clearPauseReasons(partitionPauseRewind))
	require.False(t, p.diagnostics.resumed.IsZero())
	p.diagnostics.dataReceived()
	p.diagnostics.dataReceived() // Only the first nonempty batch completes this wait.
	h := diagnosticHistogram(t, tel, "otelcol_kafka_receiver_partition_pause_duration")
	require.Len(t, h.DataPoints, 2)
	for _, dp := range h.DataPoints {
		require.Equal(t, uint64(1), dp.Count)
		require.Positive(t, dp.Sum)
	}
	h = diagnosticHistogram(t, tel, "otelcol_kafka_receiver_resume_to_data_duration")
	require.Len(t, h.DataPoints, 1)
	require.Equal(t, uint64(1), h.DataPoints[0].Count)
}

func TestPartitionDiagnosticsCanceledAndDiscardedWaits(t *testing.T) {
	p, tel := diagnosticPartition(t)
	p.addPauseReason(partitionPauseBackpressure)
	require.True(t, p.clearPauseReasons(partitionPauseBackpressure))
	p.addPauseReason(partitionPauseRewind) // A new pause interrupts the first resume interval.
	require.True(t, p.clearPauseReasons(partitionPauseRewind))
	p.retireAssignment(context.Canceled)
	p.retireAssignment(context.Canceled)
	p.diagnostics.dataReceived() // A lost assignment cannot complete a canceled wait.
	h := diagnosticHistogram(t, tel, "otelcol_kafka_receiver_resume_to_data_duration")
	require.Len(t, h.DataPoints, 2)
	outcomes := map[string]uint64{}
	for _, dp := range h.DataPoints {
		v, ok := dp.Attributes.Value("wait_outcome")
		require.True(t, ok)
		outcomes[v.AsString()] += dp.Count
	}
	require.Equal(t, map[string]uint64{"discarded": 1, "canceled": 1}, outcomes)
	p.diagnostics.processing(1)
	p.diagnostics.processing(-1)
	m, err := tel.GetMetric("otelcol_kafka_receiver_processing_records")
	require.NoError(t, err)
	require.Zero(t, m.Data.(metricdata.Gauge[int64]).DataPoints[0].Value)
}

func TestMailboxQueueDiagnostics(t *testing.T) {
	p, tel := diagnosticPartition(t)
	m := newPartitionMailbox(p.ctx, 2)
	m.diagnostics = p.diagnostics
	require.True(t, m.enqueue(mailboxBatch(0), func(partitionPauseReason) {}))
	require.True(t, m.enqueue(mailboxBatch(1), func(partitionPauseReason) {}))
	_, ok := m.dequeue(func() {})
	require.True(t, ok)
	m.discard()
	h := diagnosticHistogram(t, tel, "otelcol_kafka_receiver_mailbox_queue_duration")
	require.Len(t, h.DataPoints, 2)
	for _, dp := range h.DataPoints {
		require.Equal(t, uint64(1), dp.Count)
		require.Positive(t, dp.Sum)
	}
	gauge, err := tel.GetMetric("otelcol_kafka_receiver_mailbox_depth")
	require.NoError(t, err)
	require.Zero(t, gauge.Data.(metricdata.Gauge[int64]).DataPoints[0].Value)
}

func TestReceiverPartitionDiagnosticsOptIn(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(map[bool]string{false: "disabled", true: "enabled"}[enabled], func(t *testing.T) {
			producer, cfg := mustNewFakeCluster(t, kfake.SeedTopics(1, "otlp_spans"))
			cfg.PartitionProcessing = PartitionProcessing{Independent: true, MaxBufferedBatches: 1, MaxInFlight: MaxInFlightConfig{Records: 2}}
			cfg.Telemetry.Metrics.KafkaReceiverPartitionDiagnostics.Enabled = enabled
			cfg.Traces.Encoding = "otlp_proto"
			settings, tel, _ := mustNewSettings(t)
			received := make(chan struct{}, 1)
			r, err := newTracesReceiver(cfg, settings, newTracesConsumer(func(context.Context, ptrace.Traces) error { received <- struct{}{}; return nil }))
			require.NoError(t, err)
			require.NoError(t, r.Start(t.Context(), componenttest.NewNopHost()))
			data, err := (&ptrace.ProtoMarshaler{}).MarshalTraces(testdata.GenerateTraces(1))
			require.NoError(t, err)
			require.NoError(t, producer.ProduceSync(t.Context(), &kgo.Record{Topic: "otlp_spans", Value: data}).FirstErr())
			select {
			case <-received:
			case <-time.After(5 * time.Second):
				t.Fatal("record not delivered")
			}
			require.NoError(t, r.Shutdown(context.Background()))
			if !enabled {
				_, err = tel.GetMetric("otelcol_kafka_receiver_record_processing_duration")
				require.Error(t, err)
				return
			}
			h := diagnosticHistogram(t, tel, "otelcol_kafka_receiver_record_processing_duration")
			require.Len(t, h.DataPoints, 2)
			phases := map[string]uint64{}
			for _, dp := range h.DataPoints {
				v, ok := dp.Attributes.Value("phase")
				require.True(t, ok)
				phases[v.AsString()] += dp.Count
			}
			require.Equal(t, map[string]uint64{"decode": 1, "consume": 1}, phases)
			h = diagnosticHistogram(t, tel, "otelcol_kafka_receiver_mailbox_queue_duration")
			require.Len(t, h.DataPoints, 1)
			require.Equal(t, uint64(1), h.DataPoints[0].Count)
		})
	}
}

func TestTerminalPauseDiagnosticsSurviveWorkerStop(t *testing.T) {
	consumer, client, p := newIndependentConsumer(t, "test", 1, MessageMarking{After: true}, false)
	diagnosticPC, tel := diagnosticPartition(t)
	p.attrs = diagnosticPC.attrs
	p.diagnostics = diagnosticPC.diagnostics
	p.mailbox = newPartitionMailbox(p.ctx, 1)
	p.mailbox.diagnostics = p.diagnostics
	consumer.consumeMessage = func(context.Context, *kgo.Record, attribute.Set) error {
		return consumererror.NewPermanent(errors.New("rejected record"))
	}
	require.True(t, p.mailbox.enqueue(offsetBatch(1), func(reason partitionPauseReason) { p.addPauseReason(reason) }))
	require.True(t, p.add())
	done := make(chan struct{})
	go func() { consumer.runPartitionWorker(p, topicPartition{topic: "test", partition: 0}); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("terminal worker did not stop")
	}
	require.ErrorIs(t, p.ctx.Err(), context.Canceled)
	requireDiagnosticGauge(t, tel, "otelcol_kafka_receiver_partition_paused", 1)
	require.Contains(t, p.diagnostics.pauses, partitionPauseTerminal, "terminal pause must remain open while assignment is retained")
	consumer.lost(t.Context(), client, map[string][]int32{"test": {0}}, false)
	requireDiagnosticGauge(t, tel, "otelcol_kafka_receiver_partition_paused", 0)
	h := diagnosticHistogram(t, tel, "otelcol_kafka_receiver_partition_pause_duration")
	var terminalCount uint64
	for _, dp := range h.DataPoints {
		reason, _ := dp.Attributes.Value("pause_reason")
		if reason.AsString() == "terminal" {
			terminalCount += dp.Count
			require.Positive(t, dp.Sum)
		}
	}
	require.Equal(t, uint64(1), terminalCount)
}

func TestRetiredMailboxCannotOverwriteReplacementDepth(t *testing.T) {
	consumer, client, old := newIndependentConsumer(t, "test", 1, MessageMarking{After: true}, false)
	diagnosticPC, tel := diagnosticPartition(t)
	old.attrs = diagnosticPC.attrs
	old.diagnostics = diagnosticPC.diagnostics
	old.mailbox = newPartitionMailbox(old.ctx, 2)
	old.mailbox.diagnostics = old.diagnostics
	require.True(t, old.mailbox.enqueue(mailboxBatch(0), func(partitionPauseReason) {}))
	consumer.lost(t.Context(), client, map[string][]int32{"test": {0}}, true)
	requireDiagnosticGauge(t, tel, "otelcol_kafka_receiver_mailbox_depth", 0)
	replacement := newPartitionMailbox(t.Context(), 2)
	replacement.diagnostics = newPartitionDiagnostics(old.diagnostics.builder, old.attrs)
	require.True(t, replacement.enqueue(mailboxBatch(1), func(partitionPauseReason) {}))
	requireDiagnosticGauge(t, tel, "otelcol_kafka_receiver_mailbox_depth", 1)
	// A timed-out worker can take this cleanup path after its replacement starts.
	old.mailbox.discard()
	old.diagnostics.depth(0)
	requireDiagnosticGauge(t, tel, "otelcol_kafka_receiver_mailbox_depth", 1)
}

func requireDiagnosticGauge(t *testing.T, tel *componenttest.Telemetry, name string, want int64) {
	t.Helper()
	m, err := tel.GetMetric(name)
	require.NoError(t, err)
	gauge, ok := m.Data.(metricdata.Gauge[int64])
	require.True(t, ok)
	require.Len(t, gauge.DataPoints, 1)
	require.Equal(t, want, gauge.DataPoints[0].Value)
}
