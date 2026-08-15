import argparse
from pyspark.sql import SparkSession, DataFrame,functions as F


def add_audit_columns(df: DataFrame) -> DataFrame:
    return(
        df.withColumn("_ingested_at",F.current_timestamp())
        .withColumn("_source_file",F.col("_metadata.file_path"))
    )


def main():
    parser =argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--source_path",required=True)

    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()

    df= spark.read.option('header',True).option('inferSchema',True).csv(args.source_path)

    df = add_audit_columns(df)

    target_table = f"{args.catalog}.bronze.customer_raw"

    df.write.mode('overwrite').saveAsTable(target_table)

    print(f" Loaded {df.count()} rows  into {target_table} from {args.source_path}")




if __name__ == "__main__":
    main()