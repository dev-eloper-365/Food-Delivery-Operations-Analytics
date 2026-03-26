from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType, IntegerType, DateType
)

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=True),
    StructField("restaurant_id", StringType(), nullable=False),
    StructField("city", StringType(), nullable=True),
    StructField("order_ts", TimestampType(), nullable=False),
    StructField("promised_delivery_ts", TimestampType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("order_value", DoubleType(), nullable=True),
    StructField("payment_mode", StringType(), nullable=True),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("item_id", StringType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=True),
    StructField("item_price", DoubleType(), nullable=True),
    StructField("cuisine_type", StringType(), nullable=True),
])

DELIVERY_EVENTS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("rider_id", StringType(), nullable=True),
    StructField("event_type", StringType(), nullable=False),
    StructField("event_ts", TimestampType(), nullable=False),
    StructField("latitude", DoubleType(), nullable=True),
    StructField("longitude", DoubleType(), nullable=True),
])

RESTAURANTS_SCHEMA = StructType([
    StructField("restaurant_id", StringType(), nullable=False),
    StructField("city", StringType(), nullable=True),
    StructField("cuisine_type", StringType(), nullable=True),
    StructField("rating_band", StringType(), nullable=True),
    StructField("onboarding_date", DateType(), nullable=True),
])

RIDERS_SCHEMA = StructType([
    StructField("rider_id", StringType(), nullable=False),
    StructField("city", StringType(), nullable=True),
    StructField("shift_type", StringType(), nullable=True),
    StructField("joining_date", DateType(), nullable=True),
])

REFUNDS_SCHEMA = StructType([
    StructField("refund_id", StringType(), nullable=False),
    StructField("order_id", StringType(), nullable=False),
    StructField("refund_ts", TimestampType(), nullable=True),
    StructField("refund_reason", StringType(), nullable=True),
    StructField("refund_amount", DoubleType(), nullable=True),
])

SUPPORT_TICKETS_SCHEMA = StructType([
    StructField("ticket_id", StringType(), nullable=False),
    StructField("order_id", StringType(), nullable=False),
    StructField("ticket_type", StringType(), nullable=True),
    StructField("created_ts", TimestampType(), nullable=True),
    StructField("resolution_status", StringType(), nullable=True),
])
