from src.silver.batch_clean import clean_customers



COLS =["customer_id", "first_name", "last_name", "email", "phone",
        "address", "city", "state", "zip_code", "segment", "source_updated_at"]

def test_dedup_keeps_latest_record(spark):
    data = [
        ("1001", "John", "Doe", "j@x.com", "111", "Addr A", "City", "ST", "00000", "Gold", "2026-01-01T00:00:00"),
        ("1001", "John", "Doe", "j@x.com", "222", "Addr B", "City", "ST", "00000", "Gold", "2026-01-02T00:00:00"),
    ]

    df = spark.createDataFrame(data,COLS)

    result = clean_customers(df).collect()

    assert len(result) == 1
    assert result[0]['phone'] =='222'



def test_hash_changes_when_attribute_changes(spark):
    row1 = [("1001", "John", "Doe", "j@x.com", "111", "Addr A", "City", "ST", "00000", "Gold", "2026-01-01T00:00:00")]
    row2 = [("1001", "John", "Doe", "j@x.com", "111", "Addr B", "City", "ST", "00000", "Gold", "2026-01-01T00:00:00")]

    h1 = clean_customers(spark.createDataFrame(row1, COLS)).collect()[0]["hash_value"]
    h2 = clean_customers(spark.createDataFrame(row2, COLS)).collect()[0]["hash_value"]

    assert h1 != h2


def test_hash_stable_when_nothing_changes(spark):
    row = [("1001", "John", "Doe", "j@x.com", "111", "Addr A", "City", "ST", "00000", "Gold", "2026-01-01T00:00:00")]
    h1 = clean_customers(spark.createDataFrame(row, COLS)).collect()[0]["hash_value"]
    h2 = clean_customers(spark.createDataFrame(row, COLS)).collect()[0]["hash_value"]
    assert h1 == h2
