# insert_student_data.py
from datetime import datetime
import pyarrow as pa

from test_om.utils import catalog
# Configure Catalog (same as create script)

# Load existing table
table = catalog.load_table("school.students")

# Sample student data
students = [
    {
        "student_id": 101,
        "name": "Alice Johnson",
        "department": "Computer Science",
        "enrollment_date": datetime(2023, 9, 1),
        "gpa": 3.8
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

# Append data
table.append(arrow_table)

print("✅ Data inserted successfully!")
print(table.scan().to_arrow().to_pandas())