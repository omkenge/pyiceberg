import datetime
import pyarrow.fs as fs
from pyiceberg.catalog import load_catalog

# Load catalog and table.
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
table = catalog.load_table("school.students")


def list_data_files_from_table(table_location: str) -> set:
    """
    Given a table location, list all files under the 'data' subdirectory.
    
    If table_location already ends with 'data', it uses that path directly.
    
    Parameters:
      table_location (str): The table location, e.g.
          's3://warehouse/school/students' or 's3://warehouse/school/students/data'
    
    Returns:
      set: A set of file paths (S3 URIs) for files found in the data directory.
    """
    if not table_location.startswith("s3://"):
        raise ValueError("Table location must start with 's3://'")
    
    base = table_location.rstrip("/")
    if not base.endswith("/data"):
        data_location = f"{base}/data"
    else:
        data_location = base

    # Create an S3FileSystem instance using the connection details.
    s3 = fs.S3FileSystem(
        region="eu-central-1",
        endpoint_override="127.0.0.1:9000",
        access_key="admin",
        secret_key="password",
        scheme="http"
    )

    # Remove the "s3://" scheme and split into bucket and prefix.
    without_scheme = data_location[5:]
    parts = without_scheme.split("/", 1)
    bucket = parts[0]
    prefix = parts[1] if len(parts) > 1 else ""
    
    full_path = f"{bucket}/{prefix}" if prefix else bucket
    selector = fs.FileSelector(full_path, recursive=True)
    
    file_infos = s3.get_file_info(selector)
    file_paths = {f"s3://{info.path}" for info in file_infos if info.type == fs.FileType.File}
    
    return file_paths


def extract_metadata_files(table) -> set:
    """
    Use table.inspect.files() to get a PyArrow table of file metadata,
    extract the 'file_path' column, and return a set of file paths.
    
    The PyArrow table is assumed to have a column named 'file_path' that
    contains the file URIs (or paths).
    """
    metadata_table = table.inspect.files()
    # Extract the column named "file_path"
    file_paths_list = metadata_table.column("file_path").to_pylist()
    metadata_files = set(file_paths_list)
    return metadata_files


def delete_orphan_files(orphan_files, retention_days=30, dry_run=True):
    """
    Delete orphan files from S3 that are older than the retention period.

    Parameters:
      orphan_files (set): Set of orphan file paths.
      retention_days (int): Number of days to keep orphan files before deleting.
      dry_run (bool): If True, only prints the files that would be deleted.
    """
    s3 = fs.S3FileSystem(region="us-west-2")  # Adjust region as needed
    retention_interval = datetime.timedelta(days=retention_days)
    now = datetime.datetime.now(tz=datetime.timezone.utc)

    for file in orphan_files:
        try:
            # Remove "s3://" and extract bucket/key
            bucket, key = file[5:].split("/", 1)
            full_key = f"{bucket}/{key}"

            # Retrieve file info
            file_info = s3.get_file_info(full_key)

            # Convert last modified timestamp from milliseconds to seconds
            last_modified_dt = datetime.datetime.fromtimestamp(file_info.mtime / 1000.0, tz=datetime.timezone.utc)

            # Check if file is older than retention interval
            if now - last_modified_dt >= retention_interval:
                if dry_run:
                    print(f"[Dry Run] Would delete: {file} (last modified: {last_modified_dt.isoformat()})")
                else:
                    print(f"Deleting: {file} (last modified: {last_modified_dt.isoformat()})")
                    s3.delete_file(full_key)
            else:
                print(f"Skipping (not old enough): {file} (last modified: {last_modified_dt.isoformat()})")
        
        except Exception as e:
            print(f"Error processing {file}: {e}")


# ----- Main Workflow -----

# 1. List files from S3 (data directory).
table_location = table.location()  # e.g., "s3://warehouse/school/students"
s3_files = list_data_files_from_table(table_location)
print("Data files found on S3:")
for file in sorted(s3_files):
    print(file)

# 2. Extract metadata files using table.inspect.files()
metadata_files = extract_metadata_files(table)
print("\nData files referenced in table metadata (via inspect.files()):")
for file in sorted(metadata_files):
    print(file)

# 3. Identify orphan files (present in S3 but not in metadata).
orphan_files = s3_files - metadata_files
print("\nOrphan files (present on S3 but not in table metadata):")
for file in sorted(orphan_files):
    print(file)

# 4. Delete orphan files that are older than the retention interval.
# Create an S3FileSystem instance to pass into the deletion function.
s3 = fs.S3FileSystem(
    region="eu-central-1",
    endpoint_override="127.0.0.1:9000",
    access_key="admin",
    secret_key="password",
    scheme="http"
)

# Define a retention interval (e.g., 3 days). Files newer than this will not be deleted.
# retention_interval = datetime.timedelta(days=3)

# Set dry_run to False to perform actual deletions.
delete_orphan_files(orphan_files, retention_days=7, dry_run=False)
