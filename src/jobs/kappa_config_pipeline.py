from __future__ import annotations

import argparse
import json
from typing import Iterable, List

from pyspark.sql import DataFrame

from src.common.config_io import write_iceberg
from src.common.kappa_layer_processor import KappaLayerProcessor
from src.common.kappa_openmetadata import OpenMetadataEmitter
from src.common.kappa_registry import KappaFlow, KappaRegistry, kappa_registry_summary, load_kappa_registry
from src.common.kappa_transform import extract_debezium_payload
from src.common.spark_session import build_spark


class KappaConfigPipeline:
    """
    Kappa runtime with two physical execution styles:

    - stream-full: Kafka -> Raw -> Work -> Quarantine/Silver in one Spark Structured Streaming job.
      This is recommended for the thesis prototype because it keeps low latency and simple execution.

    - stream-raw-only: Kafka -> Raw only. This is recommended for production-like layered operation
      when Raw ingestion SLA and Silver governance SLA should be scaled/retried independently.

    The transformation semantics are still centralized in KappaLayerProcessor, so both execution styles
    use the same metadata-driven rules.
    """

    def __init__(self, registry: KappaRegistry):
        self.registry = registry
        self.spark = build_spark(registry.runtime.app_name)
        try:
            self.metadata_emitter = OpenMetadataEmitter.from_file()
        except Exception:
            self.metadata_emitter = None
        self.layer_processor = KappaLayerProcessor(self.spark, registry, self.metadata_emitter)

    def close(self) -> None:
        self.spark.stop()

    def read_kafka_stream(self, flow: KappaFlow) -> DataFrame:
        conn = self.registry.connections[flow.source["connection"]]
        opts = conn.options
        return (
            self.spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", opts["bootstrap_servers"])
            .option("subscribe", ",".join(flow.topic_list))
            .option("startingOffsets", opts.get("starting_offsets", "latest"))
            .option("failOnDataLoss", str(opts.get("fail_on_data_loss", False)).lower())
            .load()
        )

    def build_raw(self, kafka_df: DataFrame, flow: KappaFlow) -> DataFrame:
        fmt = flow.source.get("event_format", "debezium")
        if fmt != "debezium":
            raise ValueError(f"Only Debezium event_format is implemented in this prototype, got {fmt}")
        return extract_debezium_payload(kafka_df, flow, self.registry)

    def process_full_batch(self, batch_df: DataFrame, batch_id: int, flow: KappaFlow) -> None:
        if batch_df.rdd.isEmpty():
            return
        raw_df = self.layer_processor.write_raw(batch_df, flow, batch_id)
        self.layer_processor.process_raw_to_silver(raw_df, flow, batch_id)

    def process_raw_only_batch(self, batch_df: DataFrame, batch_id: int, flow: KappaFlow) -> None:
        if batch_df.rdd.isEmpty():
            return
        self.layer_processor.write_raw(batch_df, flow, batch_id)

    def start_flow(self, flow: KappaFlow, *, raw_only: bool = False):
        kafka_df = self.read_kafka_stream(flow)
        raw_df = self.build_raw(kafka_df, flow)
        suffix = "raw_only" if raw_only else "full"
        checkpoint = f"{self.registry.runtime.checkpoint_base}/{flow.name}/{suffix}"
        trigger = flow.source.get("trigger", self.registry.runtime.default_trigger)
        process_fn = self.process_raw_only_batch if raw_only else self.process_full_batch
        return (
            raw_df.writeStream.foreachBatch(lambda df, bid: process_fn(df, bid, flow))
            .option("checkpointLocation", checkpoint)
            .trigger(processingTime=trigger)
            .queryName(f"kappa_{suffix}_{flow.name}")
            .start()
        )

    def start_flows(self, flows: Iterable[KappaFlow], *, raw_only: bool = False) -> None:
        queries = [self.start_flow(flow, raw_only=raw_only) for flow in flows]
        for query in queries:
            query.awaitTermination()

    def run_models_once(self, model_names: List[str] | None = None) -> None:
        selected = self.registry.enabled_models(model_names)
        catalog = self.registry.runtime.catalog
        for model in selected:
            df = self.spark.sql(model.sql)
            target = f"{catalog}.{model.layer}.{model.name}"
            write_iceberg(df, target, write_mode=model.write_mode)
            if self.metadata_emitter:
                self.metadata_emitter.register_model(model, self.registry)
                for upstream in model.upstream:
                    source_ref = upstream if upstream.count(".") >= 2 else f"{catalog}.{upstream}"
                    from src.common.kappa_openmetadata import table_fqn
                    self.metadata_emitter.add_lineage(
                        table_fqn(self.metadata_emitter.cfg, source_ref, catalog),
                        table_fqn(self.metadata_emitter.cfg, target, catalog),
                        model.name,
                        "Gold/Mart model generated from configured SQL.",
                    )
            print(json.dumps({"model": model.name, "target": target, "rows": df.count()}))


def parse_csv(value: str | None) -> List[str]:
    return [x.strip() for x in (value or "").split(",") if x.strip()]


def main() -> None:
    ap = argparse.ArgumentParser(description="Run metadata-configured Kappa Lakehouse pipeline")
    ap.add_argument("--config", default="metadata/kappa_flows.yaml")
    ap.add_argument("--flows", default="", help="Comma-separated flow names; empty means all enabled flows")
    ap.add_argument("--models", default="", help="Comma-separated model names")
    ap.add_argument(
        "--mode",
        default="stream-raw-only",
        choices=["summary", "stream-raw-only", "models-once", "stream", "stream-full"],
        help="v7 default is stream-raw-only; stream/stream-full are deprecated compatibility modes",
    )
    args = ap.parse_args()

    registry = load_kappa_registry(args.config)
    if args.mode == "summary":
        print(json.dumps(kappa_registry_summary(registry), indent=2, ensure_ascii=False))
        return

    pipeline = KappaConfigPipeline(registry)
    try:
        if args.mode == "models-once":
            pipeline.run_models_once(parse_csv(args.models))
        elif args.mode == "stream-raw-only":
            pipeline.start_flows(registry.enabled_flows(parse_csv(args.flows)), raw_only=True)
        else:
            pipeline.start_flows(registry.enabled_flows(parse_csv(args.flows)), raw_only=False)
    finally:
        pipeline.close()


if __name__ == "__main__":
    main()
