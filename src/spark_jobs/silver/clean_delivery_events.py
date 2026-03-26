from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, row_number
from pyspark.sql.window import Window


class DeliveryEventsCleaner:
    VALID_EVENT_TYPES = [
        "order_confirmed", "restaurant_accepted", "food_prep_started",
        "food_ready", "rider_assigned", "rider_picked_up",
        "out_for_delivery", "delivered", "delivery_failed", "cancelled"
    ]

    def __init__(self, spark: SparkSession, bronze_path: str, silver_path: str):
        self.spark = spark
        self.bronze_path = bronze_path
        self.silver_path = silver_path

    def read_bronze(self) -> DataFrame:
        return self.spark.read.parquet(f"{self.bronze_path}/delivery_events")

    def clean(self, df: DataFrame) -> DataFrame:
        return (df
            .transform(self._remove_corrupt_records)
            .transform(self._deduplicate)
            .transform(self._add_data_quality_flags))

    def _remove_corrupt_records(self, df: DataFrame) -> DataFrame:
        if "_corrupt_record" in df.columns:
            return df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
        return df

    def _deduplicate(self, df: DataFrame) -> DataFrame:
        window = (Window
            .partitionBy("order_id", "event_type")
            .orderBy(col("_ingested_at").desc()))
        return (df
            .withColumn("_row_num", row_number().over(window))
            .filter(col("_row_num") == 1)
            .drop("_row_num"))

    def _add_data_quality_flags(self, df: DataFrame) -> DataFrame:
        try:
            orders_df = self.spark.read.parquet(f"{self.bronze_path}/orders")
            order_ids = [row.order_id for row in orders_df.select("order_id").distinct().collect()]
            df = df.withColumn("_dq_orphan_order", ~col("order_id").isin(order_ids))
        except Exception:
            df = df.withColumn("_dq_orphan_order", lit(False))

        return (df
            .withColumn("_dq_invalid_event_type", ~col("event_type").isin(self.VALID_EVENT_TYPES))
            .withColumn("_dq_missing_rider",
                (col("event_type").isin(["rider_assigned", "rider_picked_up",
                    "out_for_delivery", "delivered"])) & col("rider_id").isNull()))

    def write_silver(self, df: DataFrame) -> None:
        (df.write.mode("overwrite").parquet(f"{self.silver_path}/delivery_events"))
        print(f"  Silver delivery_events written ({df.count()} rows)")
