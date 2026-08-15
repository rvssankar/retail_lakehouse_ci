import argparse
from pyspark.sql import SparkSession, DataFrame, functions as F
from delta.tables import DeltaTable


def apply_scd2(spark: SparkSession,source_df:DataFrame,gold_table:str) -> None:

    if not spark.catalog.tableExists(gold_table):
        (
            source_df.withColumn("start_date",F.current_timestamp())
            .withColumn("end_date",F.lit(None).cast("timestamp"))
            .withColumn("is_current",F.lit(True))
            .write.format('delta').mode('overwrite').saveAsTable(gold_table)
        )
        return

    target_table =DeltaTable.forName(spark,gold_table)

    #Step 1: Expire current rows whose hash value is changed:
    (
        target_table.alias('t')
        .merge(source_df.alias('s'), "t.customer_id =s.customer_id  AND t.is_current =True")
        .whenMatchedUpdate(condition="t.hash_value !=s.hash_value",
                           set={"is_current":F.lit(False), "end_date":F.current_timestamp()},)
        .execute()
    )

    # Step 2: Insert new rows from source  for changed-customers and new customers:

    current_keys = spark.table(gold_table).filter("is_current=True").select("customer_id", "hash_value")

    to_insert = (
        source_df.alias('s')
        .join(current_keys.alias('c'),on=["customer_id"], how="left")
        .filter("c.customer_id is NULL or c.hash_value != s.hash_value")
        .select("s.*")
        .withColumn("start_date",F.current_timestamp())
        .withColumn("end_date",F.lit(None).cast("timestamp"))
        .withColumn("is_current",F.lit(True))
    )

    to_insert.write.format('delta').mode('append').saveAsTable(gold_table)


def main():
    parser =argparse.ArgumentParser()
    parser.add_argument("--catalog",required=True)
    args = parser.parse_args()

    spark =SparkSession.builder.getOrCreate()

    silver_tbl =f"{args.catalog}.silver.customer_clean"
    gold_tbl =f"{args.catalog}.gold.dim_customer"

    apply_scd2(spark=spark,source_df=spark.table(silver_tbl), gold_table=gold_tbl)

    print(f"SCD merge completed for {gold_tbl}")



if __name__ == "__main__":
    main()


