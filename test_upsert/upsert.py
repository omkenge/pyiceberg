from datetime import datetime
import pyarrow as pa
from pyiceberg.catalog import load_catalog
from pyiceberg.table import upsert_util
from typing import List, Optional

# Configure Catalog
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
Table = catalog.load_table("school.students")

# Test cases for upsert function
def test_upsert():
    test_data = [
        {"student_id": 101, "name": "Alice Johnson", "department": "Computer Science", "enrollment_date": datetime(2023, 9, 1), "gpa": 3.9},
        {"student_id": 103, "name": "Charlie Brown", "department": "Physics", "enrollment_date": datetime(2024, 2, 20), "gpa": 3.7},
    ]
    
    arrow_table = pa.Table.from_pylist(
        test_data,
        schema=pa.schema([
            ("student_id", pa.int32(), False),
            ("name", pa.string(), False),
            ("department", pa.string(), False),
            ("enrollment_date", pa.timestamp("us"), False),
            ("gpa", pa.float64(), False),
        ]),
    )

    print("Before Upsert:")
    print(Table.scan().to_arrow().to_pandas())

    result = Table.upsert(
        df=arrow_table,
        join_cols=["student_id"],
        when_matched_update_all=True,
        when_not_matched_insert_all=True,
    )
    
    print(f"Upsert result: {result.rows_updated} rows updated, {result.rows_inserted} rows inserted")
    
    print("After Upsert:")
    print(Table.scan().to_arrow().to_pandas())

if __name__ == "__main__":
    test_upsert()
