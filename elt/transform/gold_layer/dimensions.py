from configs.config import *
import pyarrow as pa
from datetime import date
spark = spark_builder("Gold")

def reorder_columns(df,sk):
    first_col = sk
    columns = list(set(df.columns)-set(first_col))
    final_order = first_col + columns
    return df.select(*final_order)

def add_dummy_row(df):
    num_not_sk = len(df.columns) - 1
    schema = df.schema
    dummy_row = [('-1', *[None] * num_not_sk)]
    dummy_df = spark.createDataFrame(dummy_row, schema=schema)
    return df.union(dummy_df)


def reference_dimensions():
     for ref, values in get_config("reference")["references"].items():
         if ref == "cities":
             id_col = "city_id"
         elif ref == "operators":
             id_col = "operator_id"
         elif ref == "plans":
             id_col = "plan_id"
         else:
             id_col = "zone_id"
         if values["scd_type"] == 1 :
             dim = spark.read.parquet(f"s3a://{bucket}/silver/{ref}/")
             dim = dim.withColumn(f"{ref}_sk", sha2(col(f"{id_col}"),256))
             dim.write.parquet(f"s3a://{bucket}/gold/dim_{ref}/", mode="overwrite")
         else:
             dim = spark.read.parquet(f"s3a://{bucket}/silver/{ref}/")
             dim = dim.withColumn(f"{ref}_sk", sha2(concat_ws("|",col(f"{id_col}"),col("valid_from").cast(StringType())),256))
             dim.write.parquet(f"s3a://{bucket}/gold/dim_{ref}/", mode="overwrite")


last_ts = lit("9999-12-31 00:00:00").cast("timestamp")


def dimension_vehicle():
    dim_vehicle = spark.read.parquet(f"s3a://{bucket}/silver/vehicles/")
    change_tracker = ["firmware_version", "home_city_id","qr_code","model","battery_wh","operator_id", "vehicle_type","top_speed_kph","manufacturer", "status","purchase_date" ]
    w = Window.partitionBy("vehicle_id").orderBy("snapshot_date")

    dim_vehicle = dim_vehicle.withColumn("state_hash", sha2(concat_ws("|", *[coalesce(col(c).cast(StringType()),lit("~")) for c in change_tracker]),256))
    dim_vehicle = dim_vehicle.withColumn("last_hash", lag(col("state_hash")).over(w))
    dim_vehicle = dim_vehicle.filter(col("last_hash").isNotNull() | (col("state_hash")!= col("last_hash")))
    dim_vehicle = dim_vehicle.withColumn("valid_from_ts", to_timestamp(col("snapshot_date")))
    dim_vehicle = dim_vehicle.withColumn("valid_to_ts", coalesce((lead(col("valid_from_ts"))).over(w),last_ts))
    dim_vehicle = dim_vehicle.withColumn("is_current", (col("valid_to_ts")==last_ts).cast(BooleanType()))
    dim_vehicle = dim_vehicle.drop("last_hash")
    dim_vehicle = dim_vehicle.withColumn("vehicle_sk", sha2(concat_ws("|",col("vehicle_id"),col("valid_from_ts").cast(StringType())),256))
    dim_vehicle = reorder_columns(dim_vehicle,["vehicle_sk"])
    dim_vehicle = add_dummy_row(dim_vehicle)
    dim_vehicle.write.parquet(f"s3a://{bucket}/gold/dim_vehicles/", mode="overwrite")

def dimension_riders_cdc():
    dim_riders = spark.read.parquet(f"s3a://{bucket}/silver/riders_cdc/")
    changing_attributes = ["country","marketing_consent","plan_id","operator_id","home_city_id"]
    w = Window.partitionBy("rider_id").orderBy("op_ts")
    dim_riders = dim_riders.withColumn("change_hash", sha2(concat_ws("|",*[col(c).cast(StringType()) for c in changing_attributes]),256))
    dim_riders = dim_riders.withColumn("last_hash", lag(col("change_hash")).over(w))
    dim_riders = dim_riders.filter(col("last_hash").isNotNull() | (col("change_hash")!= col("last_hash")) | (col("op") == lit("D")))
    dim_riders = dim_riders.withColumn("valid_from_ts", col("op_ts"))
    dim_riders = dim_riders.withColumn("valid_to_ts", coalesce((lead(col("valid_from_ts"))).over(w),last_ts))
    dim_riders = dim_riders.filter(col("op") != "D").withColumn("is_current",(col("valid_to_ts")==last_ts).cast(BooleanType()))
    dim_riders = dim_riders.drop("last_hash","change_hash")
    dim_riders = dim_riders.withColumn("riders_sk", sha2(concat_ws("|",col("rider_id"),col("valid_from_ts").cast(StringType())),256))
    dim_riders = reorder_columns(dim_riders,["riders_sk"])
    dim_riders = add_dummy_row(dim_riders)
    dim_riders.write.parquet(f"s3a://{bucket}/gold/dim_riders/", mode="overwrite")

def dim_date():
    dim_dates = spark.createDataFrame([("2026-01-01","2026-12-31")],["start","end"])
    dim_dates = dim_dates.withColumns({"start": to_date(col("start")),"end": to_date(col("end"))})
    dim_dates = dim_dates.select(explode(sequence(col("start"),col("end"))).alias("date"))
    dim_dates = dim_dates.withColumns({
        "month": month(col("date")),
        "year": year(col("date")),
        "day": day(col("date")),
        "day_week": weekday(col("date")),
        "is_weekend": weekday(col("date")) >= 5
    })
    dim_dates = dim_dates.withColumn("date_sk", sha2(col("date").cast(StringType()),256))
    dim_dates = reorder_columns(dim_dates, ["date_sk"])
    dim_dates = add_dummy_row(dim_dates)
    dim_dates.write.parquet(f"s3a://{bucket}/gold/dim_dates/", mode="overwrite")

def dim_time_of_day():
    frmt = "HH:mm:ss"
    dim_time_of_day = spark.createDataFrame([("00:00:00","23:59:59")],["start","end"])
    dim_time_of_day = dim_time_of_day.select(explode(sequence(to_timestamp(col("start"),frmt),to_timestamp(col("end"),frmt),expr("interval 1 second"))).alias("time"))
    dim_time_of_day = dim_time_of_day.withColumn("time_sk", sha2(col("time").cast(StringType()),256))
    dim_time_of_day = reorder_columns(dim_time_of_day, ["time_sk"])
    dim_time_of_day = add_dummy_row(dim_time_of_day)
    dim_time_of_day.write.parquet(f"s3a://{bucket}/gold/dim_time_of_day/", mode="overwrite")

def dimension_payment_method():
    dim_payment_method = spark.read.parquet(f"s3a://{bucket}/silver/payments/").select("method").distinct()
    dim_payment_method = dim_payment_method.withColumn("payment_method_sk", hash(col("method")))
    dim_payment_method = reorder_columns(dim_payment_method, ["payment_method_sk"])
    dim_payment_method = add_dummy_row(dim_payment_method)
    dim_payment_method.write.parquet(f"s3a://{bucket}/gold/dim_payment_method/", mode="overwrite")

def dim_weather():
    dim_weather_day = spark.read.parquet(f"s3a://{bucket}/silver/weather/")
    dim_weather_day = dim_weather_day.withColumn("weather_day_sk", sha2(concat_ws("|",col("city_id"), col("date").cast(StringType())),256))
    dim_weather_day = reorder_columns(dim_weather_day, ["weather_day_sk"])
    dim_weather_day = add_dummy_row(dim_weather_day)
    dim_weather_day.write.parquet(f"s3a://{bucket}/gold/dim_weather_day/", mode="overwrite")

def dimension_ride_context():
    dim_rides = spark.read.parquet(f"s3a://{bucket}/silver/rides/")
    distinct_unlock_channel = dim_rides.select(col("unlock_channel")).distinct()
    distinct_end_reason = dim_rides.select(col("end_reason")).distinct()
    distinct_promo = dim_rides.withColumn("is_promo",
                    when(col("promo_code").isNotNull(), lit(True)).otherwise(lit(False))).select("is_promo").distinct()
    distinct_is_test = dim_rides.select(col("is_test")).distinct()
    dim_ride_context = distinct_unlock_channel.join(distinct_end_reason, how='cross').join(distinct_promo, how='cross').join(distinct_is_test, how='cross')
    dim_ride_context = dim_ride_context.withColumn("ride_context_sk",sha2(concat_ws("|",
                                                    col("unlock_channel").cast(StringType()),
                                                          col("end_reason").cast(StringType()),
                                                          col("is_promo").cast(StringType()),
                                                           col("is_test").cast(StringType())),256))
    dim_ride_context = reorder_columns(dim_ride_context, ["ride_context_sk"])
    dim_ride_context = add_dummy_row(dim_ride_context)
    dim_ride_context.write.parquet(f"s3a://{bucket}/gold/dim_ride_context/", mode="overwrite")














