
from configs.config import *


spark = spark_builder("transform_silver")



bronze_prefixes= {
    "maintenance": "bronze/maintenance/",
    "payments": "bronze/payments/",
    "cities": "bronze/reference/cities.parquet",
    "operators": "bronze/reference/operators.parquet",
    "plans": "bronze/reference/plans.parquet",
    "zones": "bronze/reference/zones.parquet",
    "riders_cdc": "bronze/riders_cdc/",
    "rides": "bronze/rides/",
    "telemetry": "bronze/telemetry/",
    "vehicles": "bronze/vehicles/",
    "weather": "bronze/weather/",
}

def parse_timestamp(c:str,tz="timezone"):

    return(
        when(col(c).rlike(r"^\d+$"), timestamp_millis(col(c).cast(LongType())))
        .when(col(c).rlike(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$"), to_utc_timestamp(col(c).cast(TimestampType()),col(tz)))
        .when(col(c).rlike(r"(Z|[+-]\d{2}:\d{2})$"),try_to_timestamp(col(c)))
        .when(col(c).rlike(r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$"),to_utc_timestamp(to_timestamp(col(c),"dd/MM/yyyy HH:mm"),col(tz)))
        .otherwise(lit(None).cast(TimestampType()))
    )




def transform_cities():
    cities_path = f"s3a://{bucket}/bronze/reference/cities.parquet"
    float_cols = ["centre_lat", "centre_lon"]
    cities = spark.read.parquet(cities_path)
    cities = cities.select([col(c).cast("float") if c in float_cols else c for c in cities.columns])
    cities = cities.withColumn("country",
                               when(col("country" ).isin(["ES","Spain","esp" ]),"ES").when(col("country").isin(["FR","France","fra"]),"FR").when(col("country").isin(["SE","Sweden","SWE"]),"SE").otherwise("PT"))
    cities = cities.withColumn("population_k", col("population_k").cast(IntegerType()))
    cities.write.parquet(f"s3a://{bucket}/silver/cities/", mode="overwrite")

def transform_operators():
    operators_path = f"s3a://{bucket}/bronze/reference/operators.parquet"
    operators = spark.read.parquet(operators_path)
    operators = operators.withColumn("active_from", to_date(col("active_from"),"yyyy-MM-dd"))
    operators = operators.withColumn("revenue_share_pct",col("revenue_share_pct").cast(DecimalType(10,1)))
    operators.write.parquet(f"s3a://{bucket}/silver/operators/",mode="overwrite")

def transform_plans():
    plans = spark.read.parquet(f"s3a://{bucket}/bronze/reference/plans.parquet")
    float_cols = ["monthly_fee_eur", "per_minute_eur","unluck_fee_eur"]
    plans = plans.select([col(c).cast(DecimalType(10,2)) if c in float_cols else col(c) for c in plans.columns])
    date_cols = ["valid_from", "valid_to"]
    plans = plans.select([try_to_date(col(c)) if c in date_cols else col(c) for c in plans.columns ])
    plans = plans.withColumn("version", col("version").cast(IntegerType()))
    plans.write.parquet(f"s3a://{bucket}/silver/plans/",mode="overwrite")


def transform_zones():
    zones = spark.read.parquet(f"s3a://{bucket}/bronze/reference/zones.parquet")
    date_cols = ["valid_from", "valid_to"]
    float_cols = ["centroid_lat", "centroid_lon"]
    zones = zones.select([try_to_date(col(c)) if c in date_cols else col(c) for c in zones.columns])
    zones = zones.select([col(c).cast(FloatType()) if c in float_cols else col(c) for c in zones.columns])
    zones = zones.withColumn("is_current", col("is_current").cast(BooleanType()))
    zones = zones.withColumn("area_km2", col("area_km2").cast(DecimalType(10,2)))
    zones = zones.withColumn("parking_capacity", col("parking_capacity").cast(IntegerType()))
    zones.write.parquet(f"s3a://{bucket}/silver/zones/",mode="overwrite")

def transform_riders_cdc():
    riders_cdc = spark.read.parquet(f"s3a://{bucket}/bronze/riders_cdc/")
    operators = spark.read.parquet(f"s3a://{bucket}/silver/operators/")
    dlr = riders_cdc.join(broadcast(operators), on = "operator_id", how= "left_anti")
    dlr = dlr.withColumn("_reject_reason",lit("invalid_fk_operator"))\
        .withColumn("_reject_ts", current_timestamp())\
        .withColumn("_table", lit("riders_cdc"))
    dlr = dlr.withColumn("riders_event_date",to_date(col("op_ts")))
    riders_cdc = riders_cdc.join(broadcast(operators), on="operator_id", how="left_semi")
    riders_cdc = riders_cdc.withColumn("email", lower(trim(col("email"))))
    deleted_accounts_emails = riders_cdc.filter(col("op")=="D").select("email")
    deleted_emails_list = [c["email"] for c in deleted_accounts_emails.collect()]
    riders_cdc = riders_cdc.withColumn("email",when(col("email").isin(deleted_emails_list), "REDACTED").otherwise(col("email")))
    riders_cdc = riders_cdc.select([when(col("email")=="REDACTED",lit(None)).otherwise(col(c)).alias(c) if c not in ["rider_id","email","op","op_ts"] else col(c) for c in riders_cdc.columns])
    riders_cdc = riders_cdc.withColumn("country", when(col("country" ).isin(["ES","Spain","esp" ]),"ES").when(col("country").isin(["FR","France","fra"]),"FR").when(col("country").isin(["SE","Sweden","SWE"]),"SE").otherwise("PT"))
    riders_cdc = riders_cdc.withColumn("op_ts", to_timestamp(col("op_ts")))
    riders_cdc = riders_cdc.withColumn("marketing_consent", col("marketing_consent").cast(BooleanType()))
    riders_cdc = riders_cdc.withColumn("is_active", when(col("account_status") == "active",lit(True)).otherwise(lit(False)).cast(BooleanType()))
    riders_cdc = riders_cdc.withColumn("birth_year", col("birth_year").cast(IntegerType()))
    riders_cdc = riders_cdc.withColumn("_ingest_date", to_date(col("_ingest_date"),"yyyy-MM-dd"))
    riders_cdc = riders_cdc.drop("ingest_date")
    riders_cdc = riders_cdc.drop_duplicates()

    riders_cdc.write.parquet(f"s3a://{bucket}/silver/riders_cdc/",mode="overwrite")
    dlr.write.parquet(f"s3a://{bucket}/_quarantine/riders_cdc/", mode="overwrite")

def transform_vehicles():
    vehicles = spark.read.parquet(f"s3a://{bucket}/bronze/vehicles/")
    operators = spark.read.parquet(f"s3a://{bucket}/silver/operators/")
    dlr = vehicles.join(broadcast(operators), on="operator_id", how="left_anti")
    dlr = dlr.withColumn("_reject_reason", lit("invalid_fk_operator")).withColumn("_reject_ts", current_timestamp()).withColumn("_table", lit("vehicles"))
    dlr = dlr.withColumn("purchase_date", to_date(col("purchase_date")))
    vehicles = vehicles.dropDuplicates(["vehicle_id", "ingest_date"])
    vehicles = vehicles.withColumn("battery_wh", col("battery_wh").cast(IntegerType()))
    vehicles = vehicles.withColumn("top_speed_kph", col("top_speed_kph").cast(FloatType()))
    vehicles = vehicles.withColumn("odometer_km", col("odometer_km").cast(FloatType()))
    vehicles = vehicles.withColumn("purchase_date", to_date(col("purchase_date")))
    vehicles = vehicles.drop("ingest_date")
    vehicles = vehicles.withColumn("_ingest_date", to_date(col("_ingest_date"),"yyyy-MM-dd"))
    vehicles.write.parquet(f"s3a://{bucket}/silver/vehicles/",mode="overwrite")
    dlr.write.parquet(f"s3a://{bucket}/_quarantine/vehicles/",mode="overwrite")



def transform_maintenance():
    data = spark.read.parquet(f"s3a://{bucket}/bronze/maintenance/")
    data = data.dropDuplicates(["id"])
    data = data.withColumn("maintenance_date", to_date(col("assigned_at")))
    data = data.withColumn("assigned_at",to_timestamp(col("assigned_at")))
    data = data.withColumn("labour_cost",col("labour_cost").cast(DecimalType(10,2)))
    data = data.withColumn("repaired_at",to_timestamp(col("repaired_at")))
    data = data.withColumn("reported_at",to_timestamp(col("reported_at")))
    data = data.withColumn("vehicle_id", col("vehicle_ref"))
    data = data.drop("vehicle_ref")
    data = data.withColumn("returned_to_service_at",to_timestamp(col("returned_to_service_at")))
    data = data.withColumn("technician", when(col("technician")=="unknown",lit(None)).otherwise(col("technician")))
    data = data.withColumn("technician",initcap(trim(col("technician"))))
    data = data.withColumn("_ingest_date", to_date(col("_ingest_date")))
    data = data.drop("ingest_date")

    out_put_key = f"silver/maintenance/"
    data.write.parquet(f"s3a://{bucket}/{out_put_key}",mode="overwrite")

def transform_weather():
    weather = spark.read.parquet(f"s3a://{bucket}/bronze/weather/")
    date_columns = ["date", "period", "_ingest_date"]
    float_columns = ["precip_mm", "tmax_c", "tmin_c", "wind_kph"]
    weather = weather.select([to_date(col(c)).alias(c) if c in date_columns else col(c) for c in weather.columns])
    weather = weather.select([col(c).cast(FloatType()).alias(c) if c in float_columns else col(c) for c in weather.columns])
    weather = weather.drop("ingest_date")
    weather = weather.withColumn("city", col("city_id"))
    weather.write.parquet(f"s3a://{bucket}/silver/weather/", mode="overwrite",partitionBy="city")

def transform_rides():

    rides = spark.read.option("mergeSchema", "true").parquet(f"s3a://{bucket}/bronze/rides/")
    cities =  spark.read.parquet(f"s3a://{bucket}/silver/city/")
    float_columns= ["end_lat","end_lon","start_lat","start_lon","fare_amount"]
    timestamp_columns = ["start_ts","end_ts", "updated_at"]
    rides = rides.withColumn("duration_s", col("duration_s").try_cast(IntegerType()))
    rides = rides.withColumns({c:col(c).try_cast(FloatType()) for c in float_columns})
    rides = rides.withColumn("city_id", trim(substring_index(col("_source_file"),"/",-1),lit("rides_0123456789-.csv")))
    cities = cities.select("city_id","timezone")
    rides = rides.join(
        cities,
        on="city_id",
        how="left"
    )
    rides = rides.withColumns({c:parse_timestamp(c) for c in timestamp_columns})
    w = Window.partitionBy("ride_id").orderBy(col("updated_at").desc_nulls_last())
    rides = rides.withColumn("_row_number", row_number().over(w)).filter(col("_row_number")==1)
    rides = rides.drop("_row_number")
    rides = rides.withColumn("ingest_date", to_date(col("ingest_date")))
    dlq_1 = rides.filter((col("start_ts").isNull()) & (col("end_ts").isNull())).withColumns(
        {"_reject_ts": current_timestamp(),
         "_reject_reason": lit("Missing Start_ts and End_ts"),
         "_table": lit("rides")}
    ).withColumn("reject_date", to_date(col("_reject_ts")))
    rides = rides.filter((col("start_ts").isNotNull()) | (col("end_ts").isNotNull()))
    rides = rides.withColumn("distance_m", when(col("distance_m").contains("km"), trim(col("distance_m"),lit("km")).cast(FloatType())*1000).otherwise(col("distance_m").try_cast(FloatType())))
    rides = rides.withColumn("distance_m", when(col("distance_m") < 0 , col("distance_m")* -1).otherwise(col("distance_m")))
    rides = rides.withColumn("open_ride", when(col("end_ts").isNull(),lit(True)).otherwise(lit(False)))
    dlq_2 = rides.filter(col("start_ts")>col("end_ts")).withColumns(
        {"_reject_ts": current_timestamp(),
         "_reject_reason": lit("Impossible End and Start Timestamps"),
         "_table": lit("rides")}
    ).withColumn("reject_date", to_date(col("_reject_ts")))
    rides = rides.withColumn("ride_date", to_date(col("start_ts")))
    rides = rides.withColumn("duration_s",when(col("duration_s").isNull(),when(col("open_ride") == False,col("end_ts").cast(DoubleType())-col("start_ts").cast(DoubleType())).otherwise(lit(None))).otherwise(col("duration_s")))
    rides = rides.withColumn("is_test", col("is_test").cast(BooleanType()))
    rides = rides.drop("ingest_date")
    rides = rides.withColumn("fare_amount_euros", when(col("currency")==lit("SEK"), col("fare_amount")*0.088).otherwise(col("fare_amount")))
    dlq = dlq_1.unionByName(dlq_2,allowMissingColumns=True)

    rides.write.parquet(f"s3a://{bucket}/silver/rides/", mode="overwrite", partitionBy="ride_date")
    dlq.write.parquet(f"s3a://{bucket}/_quarantine/rides/", mode="overwrite", partitionBy="reject_date")


def transform_payments():
    payments = spark.read.parquet(f"s3a://{bucket}/bronze/payments/")
    rides = spark.read.parquet(f"s3a://{bucket}/silver/rides/")
    payments = payments.dropDuplicates(["payment_id"])
    payments = payments.withColumns({"amount_minor":col("amount_minor").cast(LongType()),
                                     "captured_at": try_to_timestamp(col("captured_at"))})
    payments = payments.withColumn("amount",(col("amount_minor").cast(DecimalType())*0.01).cast(DecimalType(10,2)))
    payments = payments.withColumn("amount_euros", when(col("currency")==lit("SEK"), (col("amount")*0.088).cast(DecimalType(10,2)) ).otherwise(col("amount")))
    payments = payments.withColumn("ride_id", col("external_ref")).drop(col("external_ref"))
    payments = payments.withColumn("_ingest_date", to_date(col("_ingest_date")))
    payments = payments.drop("ingest_date")
    dlq = payments.join(broadcast(rides), on="ride_id", how="left_anti").withColumns({
        "_reject_reason":lit("Invalid ride_id"),
        "_reject_ts": current_timestamp(),
        "_table": lit("payments")
    }).withColumn("reject_date", to_date(col("_reject_ts")))
    payments = payments.join(broadcast(rides), on="ride_id", how="left_semi")
    payments = payments.withColumn("is_charge", when(col("type")=="charge",lit(True)).otherwise(lit(False)))
    payments = payments.withColumn("payment_date", to_date(col("captured_at")))
    payments = payments.withColumn("is_successful",when(col("status")=="captured",lit(True)).otherwise(lit(False)))
    payments = payments.drop("customer_country")
    payments.write.parquet(f"s3a://{bucket}/silver/payments/", mode="overwrite", partitionBy="payment_date")
    #CHANGE TO MODE "append" LATER ON !!!!
    dlq.write.parquet(f"s3a://{bucket}/_quarantine/payments/", mode="overwrite", partitionBy="reject_date")

def transform_telemetry():
    telemetry = spark.read.option("mergeSchema","true").parquet(f"s3a://{bucket}/bronze/telemetry/")
    vehicles = spark.read.parquet(f"s3a://{bucket}/silver/vehicles/")
    unique_vehicle_ids = [row["vehicle_id"] for row in vehicles.select(col("vehicle_id")).distinct().collect()]
    telemetry = telemetry.withColumn("is_registered_vehicle", when(col("vehicle_id").isin(unique_vehicle_ids), lit(True)).otherwise(lit(False)))
    telemetry = telemetry.dropDuplicates(["event_id"])
    telemetry = telemetry.withColumn("battery", col("battery").cast(DecimalType(10,4)))
    telemetry = telemetry.withColumns({"lat":col("lat").cast(FloatType()),
                                       "lon":col("lon").cast(FloatType())})
    telemetry = telemetry.withColumns({"lat":when((col("lat")==0) & (col("lon")==0),lit(None)).otherwise(col("lat")),
                                       "lon":when((col("lat")==0) & (col("lon")==0),lit(None)).otherwise(col("lon"))})
    telemetry = telemetry.withColumn("is_locked", col("is_locked").cast(BooleanType()))
    telemetry = telemetry.dropDuplicates(["event_id"])
    telemetry = telemetry.withColumn("speed_kph", col("speed_kph").cast(FloatType()))
    telemetry = telemetry.withColumn("ts", when(col("ts").rlike(r"^\d{13}$"),
                                                timestamp_millis(col("ts").cast(LongType()))).otherwise(
        col("ts").try_cast(TimestampType())))
    telemetry = telemetry.withColumn("event_date", to_date(col("ts")))
    telemetry = telemetry.withColumn("_ingest_date", to_date(col("_ingest_date")))
    telemetry = telemetry.drop("ingest_date")
    telemetry = telemetry.withColumn("battery", when(col("battery").rlike(r"^\d{1,2}\.\d{1}$"), col("battery").cast(DecimalType(10,2))*0.01).otherwise(col("battery").cast(DecimalType(10,4))))

    telemetry.write.parquet(f"s3a://{bucket}/silver/telemetry/",mode="overwrite", partitionBy="event_date")
























