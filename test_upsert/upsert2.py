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
        "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
    },
)

# Load existing table
table = catalog.load_table("om.students")

# Sample student data
students = [
    {
        "student_id": 1021,
        "name": "Changes Johnson",
        "department": "Computer Science",
        "enrollment_date": datetime(2023, 9, 1),
        "gpa": 3.8,
        "roll_id":13
    },
    {"student_id": 1202, "name": "Bobiii Smith", "department": "Mathsdematics", "enrollment_date": datetime(2024, 1, 15), "gpa": 3.5,"roll_id":2},
]

# Create PyArrow Table with strict schema
arrow_table = pa.Table.from_pylist(
    students,
    schema=pa.schema(
        [
            ("student_id", pa.int32(), False),
            ("name", pa.string(), False),
            ("department", pa.string(), False),
            ("enrollment_date", pa.timestamp("us"), False),
            ("gpa", pa.float64(), False),
            ("roll_id",pa.int32(),False)
        ]
    ),
)
print(table.scan().to_pandas())
table.upsert(arrow_table, join_cols=["student_id","roll_id"])
print("New")
print(table.scan().to_pandas())

