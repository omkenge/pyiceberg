# insert_student_data.py
from datetime import datetime
import pyarrow as pa
from pyiceberg.catalog import load_catalog

# Configure Catalog (same as create script)
catalog = load_catalog(
    "local",
    **{
        "uri": "http://127.0.0.1:8181",
        "s3.endpoint": "http://127.0.0.1:9000",
        "s3.access-key-id": "admin",
        "s3.secret-access-key": "password",
        "s3.region": "eu-central-1",
        "s3.path-style-access": "true",
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO"
    }
)

# Load existing table
table = catalog.load_table("school.students")

# Sample student data
students = [
    {
        "student_id": 101,
        "name": "Alice Johnson",
        "department": "Computer Science",
        "enrollment_date": datetime(2023, 9, 1),
        "gpa": 3.0
    },
    {
        "student_id": 102,
        "name": "Bob Smith",
        "department": "Mathematics",
        "enrollment_date": datetime(2024, 1, 15),
        "gpa": 3.5
    }
]

# Create PyArrow Table with strict schema
arrow_table = pa.Table.from_pylist(
    students,
    schema=pa.schema([
        ("student_id", pa.int32(), False),
        ("name", pa.string(), False),
        ("department", pa.string(), False),
        ("enrollment_date", pa.timestamp("us"), False),
        ("gpa", pa.float64(), False)
    ])
)

# Overwrite existing data with new data
table.overwrite(arrow_table)

print("✅ Data overwritten successfully!")
print(table.scan().to_arrow().to_pandas())