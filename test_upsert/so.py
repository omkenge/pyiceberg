# create_map_table_solution.py
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.schema import Schema, NestedField, MapType, StringType

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

# Create namespace "om" if missing
try:
    catalog.create_namespace("om")
except Exception:
    pass

# Define an Iceberg schema with a map field.
map_type = MapType(key_id=1001, key_type=StringType(), value_id=1002, value_type=StringType())
iceberg_schema = Schema(NestedField(field_id=1, name="my_map", field_type=map_type))
arrow_schema = pa.schema([
    pa.field("my_map", pa.map_(pa.large_string(), pa.large_string()))
])
# Create the table using the Iceberg schema
# table = catalog.create_table(
#     identifier="om.omm",
#     schema=arrow_schema,
#     location="s3://warehouse/om/map_table",
# )
table=catalog.load_table("om.omm")
# import pyarrow as pa

# # Create a plain Arrow schema without extra metadata.
# arrow_schema = pa.schema([
#     pa.field("my_map", pa.map_(pa.large_string(), pa.large_string()))
# ])

# Data is structured as a list (one row) containing a list of map entries.
data = {"my_map": [[{"key": "symbol", "value": "BTC"}]]}

pa_table = pa.Table.from_pydict(data, schema=iceberg_schema.as_arrow())
table.append(pa_table)  # errors
print(table.scan().to_arrow().to_pandas())
# print("✅ Data appended successfully!")
