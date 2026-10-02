from botocore.exceptions import ClientError
from configs.config import *


try:
    s3.head_bucket(Bucket=bucket)
    print(f"Bucket {bucket} already exists")
except ClientError:
    s3.create_bucket(Bucket=bucket)
    print(f"Bucket {bucket} created")

def obj_exists(s3, bucket:str, key:str)-> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            return False
        raise


def s3load_partitioned_data(sources:dict):
    for name, partition_list in sources.items():
        for partition in partition_list:
            path = partition[1]
            keys = "/".join(f"{key}={value}"for key, value in partition[0].items())
            key = f"landing/{name}/{keys}/{path.name}"
            if obj_exists(s3, bucket, key):
                continue
            else:
                s3.upload_file(str(path), bucket, key)


def s3_load_references(references:dict):
    for name, reference in references.items():
        path = reference[1]
        key = f"landing/reference/{path.name}"
        if obj_exists(s3, bucket, key):
            continue
        else:
            s3.upload_file(str(path), bucket, key)

