from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import NestedField, Schema
from pyiceberg.transforms import YearTransform
from pyiceberg.types import DoubleType, IntegerType, StringType, TimestampType
from datetime import datetime
import random
# Configure Catalog (MinIO + Iceberg REST)
catalog = load_catalog(
    "local",
    **{
        "uri": "http://127.0.0.1:8181",
        "s3.endpoint": "http://127.0.0.1:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.region": "eu-central-1",
        "s3.path-style-access": "true",
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
    },
)
import pyarrow as pa

df = pa.Table.from_pylist(
    [
        {"city": "Amsterdam", "lat": 52.371807, "long": 4.896029},
        {"city": "San Francisco", "lat": 37.773972, "long": -122.431297},
        {"city": "Drachten", "lat": 53.11254, "long": 6.0989},
        {"city": "Paris", "lat": 48.864716, "long": 2.349014},
    ],
)
from pyiceberg.catalog import load_catalog

# tbl=catalog.load_table("default.cities")

from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, DoubleType

schema = Schema(
    NestedField(1, "city", StringType(), required=False),
    NestedField(2, "lat", DoubleType(), required=False),
    NestedField(3, "long", DoubleType(), required=False),
)
# print(df.schema)
# print(df.to_pandas())
# print(tbl.schema()) # Count nulls in each column
new_tbl = catalog.create_table("default.new_cities", schema=schema, properties={"write.object-storage.enabled": "false", "write.data.path": "s3://warehouse/new_cities/"})
new_tbl.append(df)
print(df.to_pandas())
#tbl = catalog.create_table("default.cities", schema=schema,properties={"write.object-storage.enabled": "false", "write.data.path": "s3://warehouse/cities/"})
# tbl=catalog.load_table("default.cities")
# cleaned_df = df.filter(pa.compute.is_valid(df['city']))
#tbl.upsert(df, join_cols=["city"])
# tbl=catalog.load_table("default.cities")
#tbl.append(df)
#tbl.upsert(df,join_cols=["city"])