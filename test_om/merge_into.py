# test_merge_rows.py
from datetime import datetime
import pyarrow as pa
from pyiceberg.catalog import load_catalog

# Configure Catalog (same as previous scripts)
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

def create_arrow_table(data: list) -> pa.Table:
    """Helper to create properly typed Arrow tables"""
    return pa.Table.from_pylist(
        data,
        schema=pa.schema([
            ("student_id", pa.int32(), False),
            ("name", pa.string(), False),
            ("department", pa.string(), False),
            ("enrollment_date", pa.timestamp("us"), False),
            ("gpa", pa.float64(), False)
        ])
    )

def print_table_state():
    """Show current table contents"""
    print("\nCurrent Table State:")
    print(table.scan().to_arrow().to_pandas())
    print()

# Test Case 1: Basic Merge (Update existing + Insert new)
def test_basic_merge():
    print("=== TEST 1: Basic Merge ===")
    
    # Source data with 1 update and 1 new record
    source_data = [
        {
            "student_id": 103,  # Existing ID
            "name": "Nikhil jadhav",
            "department": "Computer Science",
            "enrollment_date": datetime(2023, 9, 1),
            "gpa": 10.0  # Updated GPA
        }
    ]
    
    source_table = create_arrow_table(source_data)
    
    # Perform merge on student_id
    result = table.merge_rows(
        df=source_table,
        join_cols=["student_id"],
        merge_options={
            "when_matched_update_all": True,
            "when_not_matched_insert_all": True
        }
    )
    
    print("Merge Result:", result)
    print_table_state()
    # Expected: 1 updated, 1 inserted

def test_duplicate_source():
    print("=== TEST 2: Duplicate Source Data ===")
    
    source_data = [
        {
            "student_id": 104,
            "name": "Duplicate 1",
            "department": "Math",
            "enrollment_date": datetime(2024, 1, 1),
            "gpa": 3.0
        },
        {
            "student_id": 104,  # Duplicate ID
            "name": "Duplicate 2",
            "department": "Physics",
            "enrollment_date": datetime(2024, 1, 1),
            "gpa": 3.5
        }
    ]
    
    source_table = create_arrow_table(source_data)
    
    result = table.merge_rows(
        df=source_table,
        join_cols=["student_id"],
    )
    
    print("Merge Result:", result)

def test_partial_merge():
    print("=== TEST 3: Partial Merge Options ===")
    
    source_data = [
        {
            "student_id": 101,  # Existing
            "name": "Alice Partial Update"
        },
        {
            "student_id": 105,  # New
            "name": "Om Kenge",
            "department": "Math",
            "enrollment_date": datetime(2025, 4, 1),
            "gpa": 9.9
        }
    ]
    
    source_table = create_arrow_table(source_data)
    
    # Only insert new records, don't update existing
    result = table.merge_rows(
        df=source_table,
        join_cols=["student_id"]
    )
    
    print("Merge Result:", result)
    print_table_state()
    # Expected: 1 inserted (ID 105), 0 updated

# Test Case 4: Missing Join Columns
def test_missing_columns():
    print("=== TEST 4: Missing Join Columns ===")
    
    # Source with incorrect column name
    source_data = [
        {
            "student_id_WRONG": 106,  # Invalid column
            "name": "Invalid Column",
            "department": "Math",
            "enrollment_date": datetime(2024, 1, 1),
            "gpa": 3.0
        }
    ]
    
    # Create table with different schema
    invalid_schema = pa.schema([
        ("student_id_WRONG", pa.int32(), False),
        ("name", pa.string(), False),
        ("department", pa.string(), False),
        ("enrollment_date", pa.timestamp("us"), False),
        ("gpa", pa.float64(), False)
    ])
    source_table = pa.Table.from_pylist(source_data, schema=invalid_schema)
    
    result = table.merge_rows(
        df=source_table,
        join_cols=["student_id"]  # Column doesn't exist in source
    )
    
    print("Merge Result:", result)
    # Expected: Error about missing columns

# Run all tests
if __name__ == "__main__":
    # Initial table state
    print("Initial Table State:")
    print_table_state()
    
    test_basic_merge()
    # test_duplicate_source()
    # test_partial_merge() # Not Supported we need whole data 
    # test_missing_columns()