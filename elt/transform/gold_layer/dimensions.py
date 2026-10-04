from configs.config import *

spark = spark_builder("Gold")


def reference_dimensions():
     for ref, values in get_config("reference")["references"].items():
         if values["scd_type"] == 1 :
             dim = spark.read.parquet(f"s3a://{bucket}/silver/{ref}/")
             dim = dim.withColumn(f"{ref}_sk", sha2(col(f"{ref}_id"),256))
             dim.write.parquet(f"s3a://{bucket}/gold/dim_{ref}/", mode="overwrite")
         else:
             dim = spark.read.parquet(f"s3a://{bucket}/silver/{ref}/")
             dim = dim.withColumn(f"{ref}_sk", sha2(concat_ws("|",col(f"{ref}_id"),col("valid_from").cast(StringType())),256))
             dim.write.parquet(f"s3a://{bucket}/gold/dim_{ref}/", mode="overwrite")

def dimension_vehicle():
    dim_vehicle = spark.read.parquet(f"s3a://{bucket}/silver/vehicles/")


