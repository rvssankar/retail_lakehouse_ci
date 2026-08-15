import argparse
from pyspark.sql import SparkSession, DataFrame, Window, functions as F


HASH_COLUMNS = [
    "first_name", "last_name", "email", "phone_number",
    "address", "city", "state", "zip_code", "segment",
]

def clean_customers(df: DataFrame)-> DataFrame:
    """DEedup on customer id and fetch the latest record based on source_updated_at timestamp."""

    w = Window.partitionBy("customer_id").orderBy(F.col("source_updated_at").desc())

    deduped = (
        df.withColumn("rn",F.row_number().over(w))
        .filter("rn ==1")
        .drop("rn")
            )
    return(
        deduped.withColumn('hash_value', F.sha2(F.concat_ws("||",*HASH_COLUMNS),256),)
    )

def main():
    parser =argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    args = parser.parse_args()

    spark =SparkSession.builder.getOrCreate()

    bronze_tbl = f"{args.catalog}.bronze.customer_raw"
    silver_tbl =f"{args.catalog}.silver.customer_clean"

    result = clean_customers(spark.table(bronze_tbl))

    result.write.mode("overwrite").saveAsTable(silver_tbl)

    print(f"Wrote {result.count()} rows to {silver_tbl}")



if __name__ == "__main__":
    main()
