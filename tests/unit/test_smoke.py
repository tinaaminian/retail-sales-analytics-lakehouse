

def test_spark_session_works(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "val"])
    assert df.count() == 2