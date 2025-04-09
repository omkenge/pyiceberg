from test_om.utils import catalog
from pyiceberg.catalog import load_catalog
table=catalog.load_table("school.students")

# Perform a scan and convert the result to a Pandas DataFrame
scan = table.UpdateStatistics()  # Uses all defaults: no filter, all columns, current snapshot
data = scan.to_arrow()  # Assuming DataScan has a to_pandas() method
print(f"Table State:{data}")

