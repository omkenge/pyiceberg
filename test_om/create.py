# create_student_table.py
from pyiceberg.schema import Schema, NestedField
from pyiceberg.types import IntegerType, StringType, DoubleType, TimestampType
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import YearTransform
from test_om.utils import catalog


catalog.create_namespace_if_not_exists("school")


# Define schema and partitioning
student_schema = Schema(
    NestedField(1, "student_id", IntegerType(), required=True),
    NestedField(2, "name", StringType(), required=True),
    NestedField(3, "department", StringType(), required=True),
    NestedField(4, "enrollment_date", TimestampType(), required=True),
    NestedField(5, "gpa", DoubleType(), required=True)
)

partition_spec = PartitionSpec(
    PartitionField(4, 1000, YearTransform(), "enrollment_year")
)

# Create table with clean path settings
table = catalog.create_table(
    identifier="school.students",
    schema=student_schema,
    partition_spec=partition_spec,
    properties={
        "write.object-storage.enabled": "false"
    }
)

print("✅ Table created successfully!")
print(f"Table location: {table.location()}")