from configs.config import *

for page in paginator.paginate(Bucket= bucket, Prefix= "bronze/telemetry/"):
    for obj in page.get("Contents", []):
        print(obj["Key"])