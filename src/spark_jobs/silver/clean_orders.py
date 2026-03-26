from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, coalesce, trim, upper, lit,
    row_number, hour, dayofweek, current_timestamp
)
from pyspark.sql.window import Window


class OrdersCleaner:
    def __init__(self, spark: SparkSession, bronze_path: str, silver_path: str):
        self.spark = spark
        self.bronze_path = bronze_path
        self.silver_path = silver_path

    def read_bronze(self) -> DataFrame:
        return self.spark.read.parquet(f"{self.bronze_path}/orders")

    def clean(self, df: DataFrame) -> DataFrame:
        return (df
            .transform(self._remove_corrupt_records)
            .transform(self._deduplicate)
            .transform(self._standardize_fields)
            .transform(self._handle_nulls)
            .transform(self._add_derived_columns)
            .transform(self._add_data_quality_flags))

    def _remove_corrupt_records(self, df: DataFrame) -> DataFrame:
        if "_corrupt_record" in df.columns:
            return df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
        return df

    def _deduplicate(self, df: DataFrame) -> DataFrame:
        window = Window.partitionBy("order_id").orderBy(col("_ingested_at").desc())
        return (df
            .withColumn("_row_num", row_number().over(window))
            .filter(col("_row_num") == 1)
            .drop("_row_num"))

    def _standardize_fields(self, df: DataFrame) -> DataFrame:
        return (df
            .withColumn("city", upper(trim(col("city"))))
            .withColumn("status", trim(col("status")))
            .withColumn("payment_mode", upper(trim(col("payment_mode")))))

    def _handle_nulls(self, df: DataFrame) -> DataFrame:
        return (df
            .withColumn("payment_mode", coalesce(col("payment_mode"), lit("UNKNOWN")))
            .withColumn("order_value",
                when(col("order_value") < 0, lit(None)).otherwise(col("order_value"))))

    def _add_derived_columns(self, df: DataFrame) -> DataFrame:
        return (df
            .withColumn("order_date", col("order_ts").cast("date"))
            .withColumn("order_hour", hour(col("order_ts")))
            .withColumn("time_slot",
                when(col("order_hour").between(8, 11), lit("morning"))
                .when(col("order_hour").between(12, 15), lit("lunch"))
                .when(col("order_hour").between(16, 19), lit("evening"))
                .when(col("order_hour").between(20, 23), lit("dinner"))
                .otherwise(lit("late_night")))
            .withColumn("day_of_week", dayofweek(col("order_ts")))
            .withColumn("is_weekend", col("day_of_week").isin([1, 7])))

    def _add_data_quality_flags(self, df: DataFrame) -> DataFrame:
        return (df
            .withColumn("_dq_has_null_customer", col("customer_id").isNull())
            .withColumn("_dq_invalid_order_value",
                (col("order_value").isNull()) | (col("order_value") <= 0))
            .withColumn("_dq_future_order", col("order_ts") > current_timestamp()))

    def write_silver(self, df: DataFrame) -> None:
        (df.write.mode("overwrite").partitionBy("order_date")
         .parquet(f"{self.silver_path}/orders"))
        print(f"  Silver orders written ({df.count()} rows)")
