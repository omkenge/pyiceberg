import pyarrow as pa
import pyarrow.compute as pc
from datetime import datetime, timedelta
import time

# Create 2,000 student records
students = []
base_date = datetime(2023, 9, 1)
for i in range(2000):
    student = {
        "student_id": 101 + i,
        "name": f"Student {i+1}",
        "department": "Computer Science" if i % 2 == 0 else "Mathematics",
        "enrollment_date": base_date + timedelta(days=i % 365),
        "gpa": round(3.0 + (i % 5) * 0.2, 2),
        "roll_id": i + 1,
    }
    students.append(student)

# Define a strict schema
schema = pa.schema([
    ("student_id", pa.int32(), False),
    ("name", pa.string(), False),
    ("department", pa.string(), False),
    ("enrollment_date", pa.timestamp("us"), False),
    ("gpa", pa.float64(), False),
    ("roll_id", pa.int32(), False),
])

# Create the original PyArrow Table with 2,000 records
arrow_table = pa.Table.from_pylist(students, schema=schema)

# Create a duplicate table by concatenating the table with itself (total 4,000 rows)
duplicate_arrow_table = pa.concat_tables([arrow_table, arrow_table])
print(f"Original table row count: {arrow_table.num_rows}")
print(f"Duplicate table row count: {duplicate_arrow_table.num_rows}")

# Old logic: Group by join columns, aggregate and filter for count > 1
def has_duplicate_rows_old(df: pa.Table, join_cols: list[str]) -> bool:
    grouped = df.select(join_cols).group_by(join_cols).aggregate([([], "count_all")])
    filtered = grouped.filter(pc.field("count_all") > 1)
    return filtered.num_rows > 0

from pyarrow_ops import drop_duplicates

def has_duplicate_rows_new(df: pa.Table, join_cols: list[str]) -> bool:
    # Select the join columns and drop duplicate rows, keeping the first occurrence.
    distinct_table = drop_duplicates(df.select(join_cols), keep='first')
    return distinct_table.num_rows < df.num_rows


# Benchmark old logic
start_old = time.time()
result_old = has_duplicate_rows_old(duplicate_arrow_table, ["student_id"])
time_old = time.time() - start_old

# Benchmark new logic
start_new = time.time()
result_new = has_duplicate_rows_new(duplicate_arrow_table, ["student_id"])
time_new = time.time() - start_new

print(f"Old logic duplicate found: {result_old} in {time_old:.6f} seconds")
print(f"New logic duplicate found: {result_new} in {time_new:.6f} seconds")
