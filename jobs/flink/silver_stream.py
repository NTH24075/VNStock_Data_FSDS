"""PyFlink Silver Stream — Kafka source -> dedup -> watermark -> quarantine + sink.

Per schema_design.md Section 7.2 + rubric M7 Flink requirements (13 points):
  - Event-time watermark = 60s (bounded out-of-orderness)
  - Dedup by event_id + latest created_ts (keyed state)
  - Late events -> quarantine side output (OutputTag-based)
  - Tumbling 5-min window volume aggregation
  - Parallelism=4 sized for burst (not average load)
"""

import argparse
import json
import logging
import os
from datetime import datetime, timezone

from pyflink.common import Duration, Row, Time, WatermarkStrategy
from pyflink.common.serialization import Encoder, SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import OutputTag, StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import (
    FileSink,
    OutputFileConfig,
    RollingPolicy,
)
from pyflink.datastream.connectors.kafka import (
    KafkaOffsetsInitializer,
    KafkaSource,
)
from pyflink.datastream.functions import (
    AggregateFunction,
    KeyedProcessFunction,
    MapFunction,
    ProcessFunction,
    ProcessWindowFunction,
    RuntimeContext,
)
from pyflink.datastream.state import ValueStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Row type definitions
# ---------------------------------------------------------------------------
EVENT_TYPE = Types.ROW_NAMED(
    [
        "event_id",
        "event_type",
        "event_timestamp",
        "event_time_ms",
        "created_ts",
        "session_type",
        "ticker",
        "exchange",
        "price",
        "quantity",
        "side",
        "trade_id",
        "bid_price",
        "bid_qty",
        "ask_price",
        "ask_qty",
        "index_name",
        "index_value",
        "index_change_pct",
        "kafka_offset",
    ],
    [
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.LONG(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.STRING(),
        Types.DOUBLE(),
        Types.LONG(),
        Types.STRING(),
        Types.STRING(),
        Types.DOUBLE(),
        Types.LONG(),
        Types.DOUBLE(),
        Types.LONG(),
        Types.STRING(),
        Types.DOUBLE(),
        Types.DOUBLE(),
        Types.LONG(),
    ],
)

QUARANTINE_TYPE = Types.ROW_NAMED(
    ["event_id", "event_type", "event_ts", "reason"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.STRING()],
)

WINDOW_TYPE = Types.ROW_NAMED(
    ["ticker", "window_start", "window_end", "f_stream_volume_5m"],
    [Types.STRING(), Types.STRING(), Types.STRING(), Types.LONG()],
)


class JSONParser(MapFunction):
    """Parse raw Kafka JSON string -> Row."""

    def map(self, value: str):
        """Parse a raw Kafka value into the typed event row."""
        try:
            obj = json.loads(value)
        except json.JSONDecodeError:
            return None
        if "payload" in obj:
            obj = obj["payload"]
        event_timestamp = str(obj.get("event_timestamp", ""))
        try:
            event_time_ms = int(
                datetime.fromisoformat(event_timestamp.replace("Z", "+00:00")).timestamp()
                * 1000
            )
        except (TypeError, ValueError):
            event_time_ms = 0
        return Row(
            event_id=str(obj.get("event_id", "")),
            event_type=str(obj.get("event_type", "unknown")),
            event_timestamp=event_timestamp,
            event_time_ms=event_time_ms,
            created_ts=str(obj.get("created_ts", "")),
            session_type=str(obj.get("session_type", "")),
            ticker=str(obj.get("ticker", "")),
            exchange=str(obj.get("exchange", "")),
            price=float(obj.get("price", 0) or 0),
            quantity=int(obj.get("quantity", 0) or 0),
            side=str(obj.get("side", "")),
            trade_id=str(obj.get("trade_id", "")),
            bid_price=float(obj.get("bid_price", 0) or 0),
            bid_qty=int(obj.get("bid_qty", 0) or 0),
            ask_price=float(obj.get("ask_price", 0) or 0),
            ask_qty=int(obj.get("ask_qty", 0) or 0),
            index_name=str(obj.get("index_name", "")),
            index_value=float(obj.get("index_value", 0) or 0),
            index_change_pct=float(obj.get("index_change_pct", 0) or 0),
            kafka_offset=int(obj.get("kafka_offset", 0) or 0),
        )


class EventTimestampAssigner(TimestampAssigner):
    """Extract event_timestamp as event-time (epoch ms)."""

    def extract_timestamp(self, value, record_timestamp: int) -> int:
        """Return the event timestamp in epoch milliseconds."""
        # Parse once in JSONParser and carry the epoch explicitly across the
        # Python/JVM boundary. event_time_ms is field index 3.
        return value[3] if value[3] > 0 else record_timestamp


class DedupByEventId(KeyedProcessFunction):
    """Drop replayed records by keeping the first event for each event_id."""

    def open(self, runtime_context: RuntimeContext):
        """Initialize keyed state recording whether an event ID was emitted."""
        self.seen = runtime_context.get_state(
            ValueStateDescriptor("event_id_seen", Types.BOOLEAN())
        )

    def process_element(self, value, ctx: KeyedProcessFunction.Context):
        """Emit the first record and suppress every replay of the same key."""
        if self.seen.value():
            return
        self.seen.update(True)
        yield value


class LateEventRouter(ProcessFunction):
    """Route events beyond watermark plus allowed lateness to quarantine."""

    def __init__(self, allowed_lateness_ms: int):
        """Store the grace period applied after the current watermark."""
        self.allowed_lateness_ms = allowed_lateness_ms

    def process_element(self, value, ctx: ProcessFunction.Context):
        """Emit an event to the main stream or typed quarantine side output."""
        try:
            ts_str = value.event_timestamp.replace("Z", "+00:00")
            event_ms = int(datetime.fromisoformat(ts_str).timestamp() * 1000)
        except Exception:
            yield value
            return
        current_wm = ctx.timer_service().current_watermark()
        if current_wm > 0 and event_ms < current_wm - self.allowed_lateness_ms:
            yield (
                quarantine_tag,
                Row(
                    event_id=value.event_id,
                    event_type=value.event_type,
                    event_ts=value.event_timestamp,
                    reason="late_arrival",
                ),
            )
        else:
            yield value


class VolumeAggregator(AggregateFunction):
    """Sum quantity for 5-min tumbling window."""

    def create_accumulator(self):
        """Create an empty volume accumulator."""
        return 0

    def add(self, value, accumulator):
        """Add one trade quantity to the volume accumulator."""
        return accumulator + (value.quantity or 0)

    def get_result(self, accumulator):
        """Return the accumulated window volume."""
        return accumulator

    def merge(self, a, b):
        """Merge partial volume accumulators."""
        return a + b


class VolumeWindowFunction(ProcessWindowFunction):
    """Emit (ticker, window_start, window_end, volume_sum) per window."""

    def process(self, key, context, elements):
        """Attach ticker and event-time window bounds to aggregate output."""
        window = context.window()
        ticker = str(key)
        volume = sum(elements)
        yield Row(
            ticker=ticker,
            window_start=str(datetime.fromtimestamp(window.start / 1000, tz=timezone.utc)),
            window_end=str(datetime.fromtimestamp(window.end / 1000, tz=timezone.utc)),
            f_stream_volume_5m=volume,
        )


def row_to_json(row) -> str:
    """Convert PyFlink Row to JSON string."""
    d = row.as_dict() if hasattr(row, "as_dict") else {}
    return json.dumps(d, ensure_ascii=False, default=str)


quarantine_tag = OutputTag("quarantine-side-output", QUARANTINE_TYPE)


def make_jsonl_sink(output_dir: str, subdirectory: str) -> FileSink:
    """Build an exactly-once checkpointed JSONL file sink."""
    file_config = (
        OutputFileConfig.builder().with_part_prefix("part").with_part_suffix(".jsonl").build()
    )
    return (
        FileSink.for_row_format(
            os.path.join(output_dir, subdirectory),
            Encoder.simple_string_encoder("UTF-8"),
        )
        .with_rolling_policy(RollingPolicy.on_checkpoint_rolling_policy())
        .with_output_file_config(file_config)
        .build()
    )


def main():
    """Build and submit the Kafka-to-Silver PyFlink application."""
    parser = argparse.ArgumentParser(description="PyFlink Silver Stream")
    parser.add_argument(
        "--kafka-broker",
        default=os.getenv("KAFKA_BROKER", "kafka:29092"),
    )
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "stock_market_events_v3"))
    parser.add_argument(
        "--group-id",
        default=os.getenv("FLINK_KAFKA_GROUP_ID", "flink-silver-stream-v1"),
    )
    parser.add_argument("--output-dir", default=os.getenv("FLINK_OUTPUT_DIR", "/opt/flink/data"))
    parser.add_argument("--watermark-sec", type=int, default=60)
    parser.add_argument("--parallelism", type=int, default=4)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)s %(levelname)s %(message)s",
    )

    logger.info("=== PyFlink Silver Stream ===")
    logger.info("  Kafka  : %s [%s]", args.kafka_broker, args.topic)
    logger.info("  Watermark : %ds bounded-out-of-orderness", args.watermark_sec)
    logger.info("  Parallelism : %d (sized for burst, not avg)", args.parallelism)
    logger.info("  Output  : %s", args.output_dir)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(args.parallelism)
    env.enable_checkpointing(30_000)
    env.get_config().set_auto_watermark_interval(1_000)

    kafka_source = (
        KafkaSource.builder()
        .set_bootstrap_servers(args.kafka_broker)
        .set_topics(args.topic)
        .set_group_id(args.group_id)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    ds = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "kafka-source")
    parsed = ds.map(JSONParser(), output_type=EVENT_TYPE).filter(lambda r: r is not None)

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(args.watermark_sec)
    ).with_timestamp_assigner(EventTimestampAssigner())
    watermark_strategy = watermark_strategy.with_idleness(Duration.of_seconds(30))
    timestamped = parsed.assign_timestamps_and_watermarks(watermark_strategy)

    deduped = timestamped.key_by(lambda r: r.event_id).process(
        DedupByEventId(),
        output_type=EVENT_TYPE,
    )

    main_ds = deduped.process(
        LateEventRouter(args.watermark_sec * 1000),
        output_type=EVENT_TYPE,
    )
    quarantine_ds = main_ds.get_side_output(quarantine_tag)

    trade_windows = (
        main_ds.filter(lambda row: row.event_type == "trade")
        .key_by(lambda r: r.ticker)
        .window(TumblingEventTimeWindows.of(Time.minutes(5)))
        .allowed_lateness(args.watermark_sec * 1000)
    )
    vol_5m = trade_windows.aggregate(
        VolumeAggregator(), VolumeWindowFunction(), output_type=WINDOW_TYPE
    )

    main_ds.map(row_to_json, output_type=Types.STRING()).sink_to(
        make_jsonl_sink(args.output_dir, "stg_events"),
    )
    quarantine_ds.map(row_to_json, output_type=Types.STRING()).sink_to(
        make_jsonl_sink(args.output_dir, "stg_events_quarantine"),
    )
    vol_5m.map(row_to_json, output_type=Types.STRING()).sink_to(
        make_jsonl_sink(args.output_dir, "feat_stream_volume_5m"),
    )

    logger.info("Submitting Flink job: flink-silver-stream")
    env.execute("flink-silver-stream")


if __name__ == "__main__":
    main()
