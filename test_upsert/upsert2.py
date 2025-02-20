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
import pyarrow as pa
from datetime import datetime

target_schema = pa.schema([
    pa.field("student_id", pa.int32(), nullable=False),
    pa.field("name", pa.string(), nullable=False),
    pa.field("department", pa.string(), nullable=False),
    pa.field("enrollment_date", pa.timestamp("us"), nullable=False),
    pa.field("gpa", pa.float64(), nullable=False),
    pa.field("roll_id", pa.int32(), nullable=False),
])
students_new = [
    {
        "student_id": 110,  # New student_id
        "name": "New Student",
        "department": "Biology",
        "enrollment_date": datetime(2023, 10, 1),
        "gpa": 3.6,
        "roll_id": 3       # New roll_id
    },
    {
        "student_id": 120,
        "name": "Another Student",
        "department": "Physics",
        "enrollment_date": datetime(2023, 11, 1),
        "gpa": 3.7,
        "roll_id": 4
    }
]

arrow_table_new = pa.Table.from_pylist(students_new, schema=target_schema)
# Expected: Both records are inserted.
students_update = [
    {
        "student_id": 101,  # Matches target record 101,1
        "name": "Alice Updated",  # Updated name
        "department": "Computer Science",
        "enrollment_date": datetime(2023, 9, 1),
        "gpa": 3.9,  # Updated GPA
        "roll_id": 1
    }
]

arrow_table_update = pa.Table.from_pylist(students_update, schema=target_schema)
# Expected: Target row with (101, 1) is updated.
students_partial = [
    {
        "student_id": 200,  # New student_id
        "name": "Partial Student",
        "department": "Chemistry",
        "enrollment_date": datetime(2024, 1, 1),
        "gpa": 3.4,
        "roll_id": 1   # roll_id 1 exists, but student_id does not match target (101,1)
    }
]

arrow_table_partial = pa.Table.from_pylist(students_partial, schema=target_schema)
# Expected: This record is inserted as new.
students_duplicate = [
    {
        "student_id": 101,
        "name": "Alice Duplicate 1",
        "department": "Computer Science",
        "enrollment_date": datetime(2023, 9, 1),
        "gpa": 3.8,
        "roll_id": 1
    },
    {
        "student_id": 101,
        "name": "Alice Duplicate 2",
        "department": "Computer Science",
        "enrollment_date": datetime(2023, 9, 1),
        "gpa": 3.8,
        "roll_id": 1
    }
]

arrow_table_dup = pa.Table.from_pylist(students_duplicate, schema=target_schema)
# Expected: The duplicate-check function (has_duplicate_rows) detects duplicates
# and the upsert logic stops, raising an error.
students_mixed = [
    # Existing record: should update target row with composite key (101, 1)
    {
        "student_id": 101,
        "name": "Alice Updated Again",
        "department": "Computer Science",
        "enrollment_date": datetime(2023, 9, 1),
        "gpa": 3.85,
        "roll_id": 1
    },
    # New record: composite key (1100, 30) not in target → insert
    {
        "student_id": 1100,
        "name": "New Student Mixed",
        "department": "Biology",
        "enrollment_date": datetime(2023, 10, 5),
        "gpa": 3.65,
        "roll_id": 30
    }
]

arrow_table_mixed = pa.Table.from_pylist(students_mixed, schema=target_schema)
# Expected:
# - The record with (101, 1) updates the existing row.
# - The record with (110, 3) is inserted as a new row.

# Sample student data
students = [
    {
        "student_id": 101,
        "name": "SK Johnson",
        "department": "Computer Science",
        "enrollment_date": datetime(2023, 9, 1),
        "gpa": 9.98,
        "roll_id":1
    },
    {"student_id": 10111, "name": "OM Smith", "department": "SK", "enrollment_date": datetime(2024, 2, 15), "gpa": 3.5,"roll_id":99},
    #{"student_id": 1042, "name": "SK Smith", "department": "SK", "enrollment_date": datetime(2024, 1, 15), "gpa": 3.5,"roll_id":233},
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
table.upsert(arrow_table_mixed, join_cols=["student_id","roll_id"])
print("New")
print(table.scan().to_pandas())

