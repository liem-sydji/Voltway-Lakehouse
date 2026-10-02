from configs.config import *
import pyarrow as pa
import pyarrow.parquet as pq
import xml.etree.ElementTree as ET
import csv, io, json
from datetime import datetime
import re


def flatten(obj, parent="", sep="_"):
    out = {}
    for k, v in obj.items():
        key = f"{parent}{sep}{k}" if parent else k
        if isinstance(v, dict):
            out.update(flatten(v, key, sep))
        else:
            out[key] = None if v is None else str(v)
    return out

def parse_vehicles(text):
    doc = json.loads(text)
    return (flatten(r) for r in doc['vehicles'])

def parse_payments(text):
    doc = json.loads(text)
    return [flatten(r) for r in doc["records"]]

def parse_weather(text):
    doc = json.loads(text)
    envelope = {k: v for k, v in doc.items() if k != "days"}
    return [flatten({**envelope, **dy}) for dy in doc["days"]]


def parse_telemetry(text):
    return [flatten(json.loads(line)) for line in text.splitlines() if line.strip()]


def parse_maintenance(text):
    root = ET.fromstring(text)
    rows = []
    for event in root.findall("event"):
        row = dict(event.attrib)
        for child in event:
            row[child.tag] = child.text
            for k, v in child.attrib.items():
                row[f"{child.tag}_{k}"] = v
        rows.append(row)
    return rows

def parse_csv(text):
    return list(csv.DictReader(io.StringIO(text)))



parsers_dict = {
    "maintenance" : (parse_maintenance, "landing/maintenance/period={dt}/"),
    "payments" : (parse_payments, "landing/payments/dt={dt}/"),
    "riders_cdc": (parse_csv, "landing/riders_cdc/dt={dt}/"),
    "rides" : (parse_csv, "landing/rides/dt={dt}/"),
    "telemetry": (parse_telemetry, "landing/telemetry/city={city}/dt={dt}/hour={hour}/"),
    "reference": (parse_csv, "landing/reference/{reference}.csv"),
    "vehicles": (parse_vehicles, "landing/vehicles/snapshot_date={dt}/"),
    "weather": (parse_weather, "landing/weather/city={city}/") }

def list_keys(prefix:str):
    pages = paginator.paginate(Bucket=bucket, Prefix=prefix, Delimiter="/")
    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Size"] > 0:
                yield obj["Key"]


def read_text(key):
    return s3.get_object(Bucket=bucket, Key=key)["Body"].read().decode("utf-8")


def write_parquet_byte(rows,source_file=None, ingest_date=None, source_system=None):
    fields = sorted({k for row in rows for k in row})
    table = pa.table({field : pa.array([row.get(field) for row in rows],type=pa.string())
                      for field in fields })
    if ingest_date is not None:
        table = table.append_column("_ingest_date", pa.array([ingest_date] * len(rows), type= pa.string()))
    if source_system is not None:
        table = table.append_column("_source_system", pa.array([source_system] * len(rows), type= pa.string()))
    if source_file is not None:
        table = table.append_column("_source_file", pa.array([source_file] * len(rows), type=pa.string()))


    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    return buffer.getvalue()

def load_references(reference):
    parser,prefix = parsers_dict["reference"]
    prefix = prefix.format(reference=reference)
    out_key = f"bronze/reference/{reference}.parquet"
    source_system = str(config["sources"]["reference"]["references"][reference]["source_system"])
    ref = write_parquet_byte(parser(read_text(prefix)), ingest_date = str(datetime.now().date().isoformat()), source_system=source_system, source_file=f"s3a://{bucket}/{prefix}")
    s3.put_object(Bucket=bucket, Key=out_key, Body=ref)


def load(source,dt=None, city=None, hr=None):
    parser, prefix = parsers_dict[source]

    if city is None and hr is None:
        prefix = prefix.format(dt=dt, period=dt)
    elif hr is None:
        prefix = prefix.format(dt=dt, period=dt, city=city)
    else:
        prefix = prefix.format(dt=dt, city=city, hour=hr)

    rows = []
    keys = list(list_keys(prefix))


    for key in keys:
        for row in parser(read_text(key)):
            row['_source_file'] = f"s3a://{bucket}/{key}"
            rows.append(row)


    if dt is None and city:
        dt = str(datetime.now().date().isoformat())
        out_key = f"bronze/{source}/ingest_date={dt}/{city}/part-0.parquet"
    elif source == "telemetry":
        out_key = f"bronze/{source}/city={city}/ingest_date={str(dt)}/hour={hr}/part-0.parquet"
    else:
        out_key = f"bronze/{source}/ingest_date={str(dt)}/part-0.parquet"

    s3.put_object(
        Bucket=bucket,
        Key=out_key,
        Body=write_parquet_byte(rows,ingest_date=dt, source_system=str(config["sources"][source]["source_system"])),
    )


def paginate_over_dts(source):
    out = []
    for page in paginator.paginate(Bucket=bucket, Prefix=f"landing/{source}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            parts = key.split("/")
            for part in parts:
                if "=" in part:
                    k, v = part.split("=", 1)
                    out.append((k, v, key))
                    break
    return out

def paginate_over_telemetry():
    out = []
    for page in paginator.paginate(Bucket = bucket, Prefix = f"landing/telemetry/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            hive_parts={}
            for part in ["city", "dt", "hour"]:
                match = re.search(rf"{part}=([^/]+)",key)
                if match:
                    hive_parts[part] = match.group(1)
            out.append((hive_parts,key))
    return out






