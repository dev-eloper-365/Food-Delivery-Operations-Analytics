from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window


class SimpleTableCleaner:
    def __init__(self, spark: SparkSession, bronze_path: str, silver_path: str,
                 table_name: str, pk_column: str):
        self.spark = spark
        self.bronze_path = bronze_path
        self.silver_path = silver_path
        self.table_name = table_name
        self.pk_column = pk_column

    def read_bronze(self) -> DataFrame:
        return self.spark.read.parquet(f"{self.bronze_path}/{self.table_name}")

    def clean(self, df: DataFrame) -> DataFrame:
        if "_corrupt_record" in df.columns:
            df = df.filter(col("_corrupt_record").isNull()).drop("_corrupt_record")
        window = (Window
            .partitionBy(self.pk_column)
            .orderBy(col("_ingested_at").desc()))
        df = (df
            .withColumn("_row_num", row_number().over(window))
            .filter(col("_row_num") == 1)
            .drop("_row_num"))
        return df

    def write_silver(self, df: DataFrame) -> None:
        (df.write.mode("overwrite").parquet(f"{self.silver_path}/{self.table_name}"))
        print(f"  Silver {self.table_name} written ({df.count()} rows)")
