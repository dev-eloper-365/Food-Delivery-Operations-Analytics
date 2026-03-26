from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, input_file_name, lit
from pyspark.sql.types import StringType, StructField

from ..common.schemas import (
    ORDERS_SCHEMA, ORDER_ITEMS_SCHEMA, DELIVERY_EVENTS_SCHEMA,
    RESTAURANTS_SCHEMA, RIDERS_SCHEMA, REFUNDS_SCHEMA,
    SUPPORT_TICKETS_SCHEMA
)


class BronzeIngestion:
    def __init__(self, spark: SparkSession, raw_path: str, bronze_path: str):
        self.spark = spark
        self.raw_path = Path(raw_path)
        self.bronze_path = Path(bronze_path)
        self.batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")

    def ingest_csv(self, filename: str, schema, output_name: str = None) -> DataFrame:
        output_name = output_name or filename.replace(".csv", "")
        extended_schema = schema.add(StructField("_corrupt_record", StringType(), True))

        df = (self.spark.read
              .option("header", "true")
              .option("mode", "PERMISSIVE")
              .option("columnNameOfCorruptRecord", "_corrupt_record")
              .schema(extended_schema)
              .csv(str(self.raw_path / filename)))

        df_with_meta = self._add_metadata(df)
        output_path = self.bronze_path / output_name
        df_with_meta.write.mode("overwrite").parquet(str(output_path))
        print(f"  Ingested {filename} -> {output_name} ({df.count()} rows)")
        return df_with_meta

    def ingest_json(self, filename: str, schema, output_name: str = None) -> DataFrame:
        output_name = output_name or filename.replace(".json", "")
        df = (self.spark.read
              .option("mode", "PERMISSIVE")
              .option("multiLine", "true")
              .schema(schema)
              .json(str(self.raw_path / filename)))

        df_with_meta = self._add_metadata(df)
        output_path = self.bronze_path / output_name
        df_with_meta.write.mode("overwrite").parquet(str(output_path))
        print(f"  Ingested {filename} -> {output_name} ({df.count()} rows)")
        return df_with_meta

    def _add_metadata(self, df: DataFrame) -> DataFrame:
        return (df
            .withColumn("_source_file", input_file_name())
            .withColumn("_ingested_at", current_timestamp())
            .withColumn("_batch_id", lit(self.batch_id)))

    def run_all(self) -> None:
        self.bronze_path.mkdir(parents=True, exist_ok=True)
        csv_mappings = [
            ("orders.csv", ORDERS_SCHEMA),
            ("order_items.csv", ORDER_ITEMS_SCHEMA),
            ("restaurants.csv", RESTAURANTS_SCHEMA),
            ("riders.csv", RIDERS_SCHEMA),
            ("refunds.csv", REFUNDS_SCHEMA),
            ("support_tickets.csv", SUPPORT_TICKETS_SCHEMA),
        ]
        for filename, schema in csv_mappings:
            self.ingest_csv(filename, schema)
        self.ingest_json("delivery_events.json", DELIVERY_EVENTS_SCHEMA)
        print(f"  Bronze ingestion complete. Batch ID: {self.batch_id}")
