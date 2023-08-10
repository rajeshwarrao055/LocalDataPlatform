from pyspark.sql import SparkSession


def read_from_spark():
    spark = SparkSession.builder \
        .appName("READ_FROM_SPARK") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", "AOAbxzELSriTadwoIwBN") \
        .config("spark.hadoop.fs.s3a.secret.key", "LnTM96i1EImstHWZgRn5F4qofJsO1c5SFT4r1unK") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .getOrCreate()
    df = spark.read.csv("s3a://test-bucket/some_file.csv")
    df.show()


if __name__ == '__main__':
    read_from_spark()