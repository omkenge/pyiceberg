# create_student_table.py
from pyiceberg.catalog import load_catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import NestedField, Schema
from pyiceberg.transforms import YearTransform
from pyiceberg.types import DoubleType, IntegerType, StringType, TimestampType

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

# Create namespace if missing
try:
    catalog.create_namespace("om")
except Exception:
    pass

# Define schema and partitioning
student_schema = Schema(
    NestedField(1, "student_id", IntegerType(), required=True),
    NestedField(2, "name", StringType(), required=True),
    NestedField(3, "department", StringType(), required=True),
    NestedField(4, "enrollment_date", TimestampType(), required=True),
    NestedField(5, "gpa", DoubleType(), required=True),
    NestedField(6, "roll_id", IntegerType(), required=True),
)

partition_spec = PartitionSpec(PartitionField(4, 1000, YearTransform(), "enrollment_year"))

# Create table with clean path settings
table = catalog.create_table(
    identifier="om.students",
    schema=student_schema,
    partition_spec=partition_spec,
    location="s3://warehouse/om",
    properties={"write.object-storage.enabled": "false"},
)

print("✅ Table created successfully!")
print(f"Table location: {table.location()}")
