from pathlib import Path
from botocore.exceptions import ClientError
PROJECT_DIR = Path(__file__).parent.parent
from elt.load.landing import s3load_partitioned_data, s3_load_references
from elt.load.load_bronze import *
from elt.transform.transform_silver import *
from configs.config import *

# 1 Resolve Source Paths
# a. solve partitioned sources

list_partitioned_sources = []
for source, values in config['sources'].items():
    if 'partitioning' in values:
        list_partitioned_sources.append(str(source))
partitioned_dic = {}
for source in list_partitioned_sources:
    partitioned_dic[source] = discover_partitioned(source)

# b. solve flat reference paths

reference_dictionary = {}
references = resolve_references()
for ref in references:
    reference_dictionary[ref[0]] = ref

#Load to landing
s3load_partitioned_data(partitioned_dic)
s3_load_references(reference_dictionary)

#Load to Bronze

#References
for ref in reference_dictionary.keys():
    load_references(ref)
#Partitioned data
partition_list = list(parsers_dict.keys())
partition_list.remove("reference")
partition_list.remove("telemetry")


source_dts_dict = {}
for partition in partition_list:
    source_dts_dict[partition] = paginate_over_dts(partition)



for src, dts in source_dts_dict.items():

    if dts[0][0]== "city":
        for dt in dts:
           load(src, city=dt[1])
    else:
        for dt in dts:
            try:
                s3.head_object(Bucket=bucket, Key=f"bronze/{src}/ingest_date={dt[1]}/part-0.parquet")
                continue
            except ClientError:

                load(src, dt=dt[1])



for keys in paginate_over_telemetry():
    try:
        key = f"bronze/telemetry/city={keys[0]['city']}/dt={keys[0]['dt']}/hour={keys[0]['hour']}/part-0.parquet"
        s3.head_object(Bucket=bucket, Key=f"bronze/telemetry/city={keys[0]['city']}/ingest_date={keys[0]['dt']}/hour={keys[0]['hour']}/part-0.parquet")
        continue
    except ClientError as e:
        load(source="telemetry", city=keys[0]['city'], dt=keys[0]['dt'], hr=keys[0]['hour'])



transform_cities()
transform_operators()
transform_plans()
transform_zones()
transform_riders_cdc()
transform_vehicles()
transform_maintenance()
transform_weather()
transform_rides()
transform_payments()
transform_telemetry()