from pyspark.sql import SparkSession


def read_from_spark():
    spark = SparkSession.builder \
        .appName("READ_FROM_SPARK") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", "iEwFAtT86AoLTezczynQ") \
        .config("spark.hadoop.fs.s3a.secret.key", "oy2Gwn7324Cw3JgHvrNHEv0HwYFjEV7Lrr3dgIte") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()
    df = spark.read.csv("s3a://test-bucket/impacted_customers_22_jan_2023.csv")
    df.show()


if __name__ == '__main__':
    read_from_spark()