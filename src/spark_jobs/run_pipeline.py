import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pyspark.sql import SparkSession
from config.settings import PATHS
from src.spark_jobs.bronze.ingest_raw import BronzeIngestion
from src.spark_jobs.silver.clean_orders import OrdersCleaner
from src.spark_jobs.silver.clean_delivery_events import DeliveryEventsCleaner
from src.spark_jobs.silver.clean_simple import SimpleTableCleaner
from src.spark_jobs.common.quality_checks import get_dq_summary, print_dq_report


def create_spark_session() -> SparkSession:
    return (SparkSession.builder
        .appName("FoodDeliveryPipeline")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.driver.memory", "4g")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate())


def run_pipeline():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    raw_path = str(PATHS["raw"])
    bronze_path = str(PATHS["bronze"])
    silver_path = str(PATHS["silver"])

    try:
        print("=" * 60)
        print("BRONZE LAYER - Ingesting raw files")
        print("=" * 60)
        bronze = BronzeIngestion(spark=spark, raw_path=raw_path, bronze_path=bronze_path)
        bronze.run_all()

        print("\n" + "=" * 60)
        print("SILVER LAYER - Cleaning and transforming")
        print("=" * 60)

        # Orders
        orders_cleaner = OrdersCleaner(spark, bronze_path, silver_path)
        orders_bronze = orders_cleaner.read_bronze()
        orders_silver = orders_cleaner.clean(orders_bronze)
        orders_cleaner.write_silver(orders_silver)
        dq = get_dq_summary(orders_silver, ["_dq_has_null_customer", "_dq_invalid_order_value", "_dq_future_order"])
        print_dq_report("orders", dq)

        # Delivery events
        events_cleaner = DeliveryEventsCleaner(spark, bronze_path, silver_path)
        events_bronze = events_cleaner.read_bronze()
        events_silver = events_cleaner.clean(events_bronze)
        events_cleaner.write_silver(events_silver)
        dq = get_dq_summary(events_silver, ["_dq_orphan_order", "_dq_invalid_event_type", "_dq_missing_rider"])
        print_dq_report("delivery_events", dq)

        # Simple tables
        simple_tables = [
            ("order_items", "item_id"),
            ("restaurants", "restaurant_id"),
            ("riders", "rider_id"),
            ("refunds", "refund_id"),
            ("support_tickets", "ticket_id"),
        ]
        for table_name, pk_col in simple_tables:
            cleaner = SimpleTableCleaner(spark, bronze_path, silver_path, table_name, pk_col)
            df = cleaner.read_bronze()
            cleaned = cleaner.clean(df)
            cleaner.write_silver(cleaned)

        print("\n" + "=" * 60)
        print("PIPELINE COMPLETE")
        print("=" * 60)

    finally:
        spark.stop()


if __name__ == "__main__":
    run_pipeline()
