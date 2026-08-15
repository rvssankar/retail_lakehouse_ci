from src.gold.scd2_merge import apply_scd2

COLS = ["customer_id", "first_name", "last_name", "email", "phone", "address",
        "city", "state", "zip_code", "segment", "source_updated_at", "hash_value"]

def make_df(spark, customer_id, hash_value, **overrides):
    row = {
        "customer_id": customer_id, "first_name": "John", "last_name": "Doe",
        "email": "j@x.com", "phone": "111", "address": "Addr A", "city": "City",
        "state": "ST", "zip_code": "00000", "segment": "Gold",
        "source_updated_at": "2026-01-01T00:00:00", "hash_value": hash_value,
    }
    row.update(overrides)
    return spark.createDataFrame([row], COLS)

def test_scd2_creates_table_on_first_load(spark):
    table = "default.test_dim_customer_create"
    spark.sql(f"DROP TABLE IF EXISTS {table}")
    apply_scd2(spark, make_df(spark, "1001", "hash_v1"), table)

    rows = spark.table(table).collect()
    assert len(rows) == 1
    assert rows[0]["is_current"] is True
    assert rows[0]["end_date"] is None
    spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_scd2_expires_old_version_on_change(spark):
    table = "default.test_dim_customer_expire"
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    apply_scd2(spark, make_df(spark, "1001", "hash_v1"), table)
    apply_scd2(spark, make_df(spark, "1001", "hash_v2", address="Addr B"), table)

    rows = spark.table(table).orderBy("start_date").collect()
    assert len(rows) == 2
    assert rows[0]["is_current"] is False
    assert rows[0]["end_date"] is not None
    assert rows[1]["is_current"] is True
    assert rows[1]["address"] == "Addr B"
    spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_scd2_no_new_version_when_unchanged(spark):
    table = "default.test_dim_customer_unchanged"
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    df = make_df(spark, "1001", "hash_v1")
    apply_scd2(spark, df, table)
    apply_scd2(spark, df, table)

    rows = spark.table(table).collect()
    assert len(rows) == 1
    assert rows[0]["is_current"] is True
    spark.sql(f"DROP TABLE IF EXISTS {table}")


def test_scd2_handles_new_customer_added_later(spark):
    table = "default.test_dim_customer_new_added"
    spark.sql(f"DROP TABLE IF EXISTS {table}")

    apply_scd2(spark, make_df(spark, "1001", "hash_v1"), table)
    apply_scd2(spark, make_df(spark, "1002", "hash_v1"), table)

    rows = spark.table(table).collect()
    assert len(rows) == 2
    assert all(r["is_current"] for r in rows)
    spark.sql(f"DROP TABLE IF EXISTS {table}")
