# create_map_table.py
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
# The map is expected to have entries with "key" and "value" fields.
map_type = MapType(key_id=1001, key_type=StringType(), value_id=1002, value_type=StringType())
iceberg_schema = Schema(NestedField(field_id=1, name="my_map", field_type=map_type))

# Create the table using the Iceberg schema
# table = catalog.create_table(
#     identifier="om.map_table",
#     schema=iceberg_schema,
#     location="s3://warehouse/om/map_table",
# )
table=catalog.load_table("om.map_table")
# Construct an Arrow table using the Iceberg-converted schema.
# Note: The provided data is incorrect—the map entry is structured as {"symbol": "BTC"}
# instead of the expected {"key": "symbol", "value": "BTC"}.
data = {"my_map": [{"symbol": "BTC"}]}
pa_table = pa.Table.from_pydict(data, schema=iceberg_schema.as_arrow())

# Attempt to append the Arrow table.
# This should trigger a schema validation error due to the missing "key" and "value" fields.
table.append(pa_table)
