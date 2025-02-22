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
from datetime import datetime, timedelta

# Generate 2000 student records
students = []
base_date = datetime(2023, 9, 1)
for i in range(2):
    student = {
        "student_id": 101 + i,
        "name": f"Student {i+1}",
        "department": "Computer Science" if i % 2 == 0 else "Mathematics",
        "enrollment_date": base_date + timedelta(days=i % 365),  # Vary the enrollment date within a year
        "gpa": round(3.0 + (i % 5) * 0.2, 2),  # Cycle GPA values between 3.0 and 3.8
        "roll_id": i + 1,
    }
    students.append(student)
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
            ("roll_id", pa.int32(), False),
        ]
    ),
)

# Append data
table.append(arrow_table)

print("✅ Data inserted successfully!")
print(table.scan().to_arrow().to_pandas())
