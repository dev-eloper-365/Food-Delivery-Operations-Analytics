from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def get_dq_summary(df: DataFrame, dq_columns: list) -> dict:
    total = df.count()
    summary = {"total_records": total}
    for dq_col in dq_columns:
        if dq_col in df.columns:
            flagged = df.filter(col(dq_col) == True).count()
            summary[dq_col] = {
                "flagged_count": flagged,
                "flagged_pct": round(flagged / total * 100, 2) if total > 0 else 0,
            }
    return summary


def print_dq_report(table_name: str, summary: dict) -> None:
    print(f"\n  DQ Report for {table_name}:")
    print(f"    Total records: {summary['total_records']}")
    for key, value in summary.items():
        if key != "total_records" and isinstance(value, dict):
            print(f"    {key}: {value['flagged_count']} ({value['flagged_pct']}%)")
