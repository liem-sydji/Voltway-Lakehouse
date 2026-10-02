import os
import yaml
from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import boto3

load_dotenv()
PROJECT_PATH = Path(__file__).parent.parent

CONFIG_PATH = Path(__file__).parent / 'configs.yaml'

with open(CONFIG_PATH) as f:
    config = yaml.safe_load(f)

def resolve_path(path):
    return (PROJECT_PATH / path).resolve()

def get_config(source: str):
    return config['sources'][source]


def discover_partitioned(source: str):
    source_config = get_config(source)
    file_type = source_config['type']
    root = resolve_path(source_config['path'])
    keys = source_config['partitioning']['keys']

    pattern =  "/".join(f"{key}=*"for key in keys) + f'/*.{file_type}'
    partitions = []

    for f in sorted(root.glob(pattern)):
        rel = f.relative_to(root)
        values = {}
        for segment, key in zip(rel.parts[:-1], keys):
            k, _, v = segment.partition('=')
            values[key] = v
        partitions.append((values,f))
    return partitions


def resolve_references():
    source_config = get_config('reference')
    out = []
    for ref, spec in source_config['references'].items():
        path = resolve_path(spec['path'])

        if not path.is_file():
            raise FileNotFoundError
        out.append((ref,path))
    return out

#Load ENV variables
s3_config = config['s3_connection']
region_name = os.getenv(s3_config['region_name'])
aws_access_key_id = os.getenv(s3_config['aws_access_key_id'])
aws_secret_access_key = os.getenv(s3_config['aws_secret_access_key'])
endpoint_url = os.getenv(s3_config['endpoint_url'])
bucket = os.getenv(s3_config['bucket'])

#Setup s3 connection
s3 = boto3.client('s3',region_name= region_name, aws_access_key_id = aws_access_key_id,
                  aws_secret_access_key = aws_secret_access_key, endpoint_url = endpoint_url)
paginator = s3.get_paginator("list_objects_v2")


HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.5.0"
AWS_SDK    = "com.amazonaws:aws-java-sdk-bundle:1.12.262"

def spark_builder(appname:str):
    spark = SparkSession.builder \
        .appName(appname) \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.5.0") \
        .config("spark.hadoop.fs.s3a.endpoint", endpoint_url) \
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id) \
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()
    return spark



