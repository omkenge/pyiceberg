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

target_schema = pa.schema(
    [
        pa.field("student_id", pa.int32(), nullable=False),
        pa.field("name", pa.string(), nullable=False),
        pa.field("department", pa.string(), nullable=False),
        pa.field("enrollment_date", pa.timestamp("us"), nullable=False),
        pa.field("gpa", pa.float64(), nullable=False),
        pa.field("roll_id", pa.int32(), nullable=False),
    ]
)

students_new = [
    {
        "student_id": 101,  # New student_id
        "name": "New Student",
        "department": "Biology",
        "enrollment_date": datetime(2023, 10, 1),
        "gpa": 3.6,
        "roll_id": 2,  # New roll_id
    },
    {
        "student_id": 102,
        "name": "Another Student",
        "department": "Physics",
        "enrollment_date": datetime(2023, 11, 1),
        "gpa": 3.7,
        "roll_id": 1,
    },
]

arrow_table_new = pa.Table.from_pylist(students_new, schema=target_schema)

# Expected: Both records are inserted.

# Sample student data
students = [
    {
        "student_id": 111,
        "name": "Odmj Johnson",
        "department": "Computer Science",
        "enrollment_date": datetime(2023, 9, 1),
        "gpa": 3.8,
        "roll_id": 50,
    },
    {
        "student_id": 222,
        "name": "Yahsh Smith",
        "department": "ds",
        "enrollment_date": datetime(2024, 2, 15),
        "gpa": 3.5,
        "roll_id": 44,
    },
]
arrow_table = pa.Table.from_pylist(
    students_new,
    schema=pa.schema(
        [
            ("student_id", pa.int32(), False),
            ("name", pa.string(), False),
            ("department", pa.string(), False),
            ("enrollment_date", pa.timestamp("us"), False),
            ("gpa", pa.float64(), False),
            ("roll_id", pa.int32(), False),
        ]
    ),
)
print(table.scan().to_pandas())
table.upsert(arrow_table_new, join_cols=["student_id", "roll_id"])
print("New")
print(table.scan().to_pandas())
