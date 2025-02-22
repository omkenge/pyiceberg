import os
import pyarrow.fs as fs
from pyiceberg.catalog import load_catalog

def list_files_from_table_subdir(table_location: str, subdir: str) -> set:
    """
    List all files under the specified subdirectory (e.g. "data" or "metadata")
    for a table whose location is an S3 URI.
    
    Args:
        table_location (str): The base table location (must start with "s3://").
        subdir (str): The subdirectory to list.
    
    Returns:
        set: A set of S3 URIs for all files under the subdirectory.
    """
    if not table_location.startswith("s3://"):
        raise ValueError("Table location must start with 's3://'")
    
    # Remove any trailing slash and ensure the full location points to the subdir.
    base = table_location.rstrip("/")
    full_location = base if base.endswith(f"/{subdir}") else f"{base}/{subdir}"
    
    # Create an S3FileSystem with the required credentials.
    s3 = fs.S3FileSystem(
         region="eu-central-1",
         endpoint_override="127.0.0.1:9000",
         access_key="admin",
         secret_key="password",
         scheme="http"
    )
    
    # Remove the "s3://" prefix and split into bucket and prefix.
    bucket, prefix = full_location[5:].split("/", 1)
    
    # List files recursively under the given prefix.
    selector = fs.FileSelector(f"{bucket}/{prefix}", recursive=True)
    file_infos = s3.get_file_info(selector)
    
    # Return the full S3 URI for each file.
    return {f"s3://{info.path}" for info in file_infos if info.type == fs.FileType.File}

def extract_all_metadata_files(table) -> set:
    """
    Extract all metadata-related files from an Iceberg table by combining:
      - Files from the snapshot inspection (column "file_path")
      - Metadata log files (column "file")
      - Manifest list files from snapshots (column "manifest_list")
    
    Args:
        table: An Iceberg table object.
        
    Returns:
        set: A set of all metadata file paths.
    """
    # Extract file paths from the current snapshot.
    metadata_table = table.inspect.files()
    table_files = set(metadata_table.column("file_path").to_pylist())

    # Extract metadata log files.
    metadata_manifest = table.inspect.metadata_log_entries()
    manifest_files = set(metadata_manifest.column("file").to_pylist())

    # Extract manifest list files from snapshots.
    metadata_snapshot = table.inspect.snapshots()
    snapshot_manifests = set(metadata_snapshot.column("manifest_list").to_pylist())

    # Combine all sets into one.
    all_metadata_files = table_files.union(manifest_files).union(snapshot_manifests)
    return all_metadata_files

def collect_table_files(table) -> list:
    """
    Collect files from the table's base location by listing both the "metadata"
    and "data" subdirectories, then combine them in a single list.
    
    Args:
        table: An Iceberg table object with .location().
    
    Returns:
        list: A list of S3 URIs for all metadata and data files.
    """
    base_location = table.location()
    
    # List files in the "metadata" subdirectory.
    metadata_files = list_files_from_table_subdir(base_location, "metadata")
    
    # List files in the "data" subdirectory.
    data_files = list_files_from_table_subdir(base_location, "data")
    
    # Combine both sets into a single list.
    all_files = list(metadata_files.union(data_files))
    return all_files

def find_orphan_files(table_location: str, table) -> set:
    """
    Identify orphan files that exist in the S3 "data" subdirectory but are not
    referenced in the snapshot's metadata.
    
    Args:
        table_location (str): The base table location (must start with "s3://").
        table: An Iceberg table object.
    
    Returns:
        set: A set of orphan file URIs.
    """
    # List data files from the S3 "data" subdirectory.
    s3_data_files = list_files_from_table_subdir(table_location, "data")
    
    # Extract metadata files from the current snapshot.
    metadata_files = extract_all_metadata_files(table)
    
    # Orphan files are those in S3 data that are not present in the metadata.
    orphan_files = s3_data_files - metadata_files
    return orphan_files

def delete_orphan_files(table, dry_run=True) -> set:
    """
    Delete orphan files from the table's S3 "data" subdirectory.
    
    If dry_run is True, only prints the files that would be deleted without
    actually deleting them.
    
    Args:
        table: An Iceberg table object.
        dry_run (bool): Whether to perform a dry run.
    
    Returns:
        set: The set of orphan file URIs that were (or would be) deleted.
    """
    table_location = table.location()
    orphan_files = find_orphan_files(table_location, table)
    
    if dry_run:
        print("Dry Run: The following orphan files would be deleted:")
        for file_uri in orphan_files:
            print(file_uri)
    else:
        s3 = fs.S3FileSystem(
            region="eu-central-1",
            endpoint_override="127.0.0.1:9000",
            access_key="admin",
            secret_key="password",
            scheme="http"
        )
        for file_uri in orphan_files:
            relative_path = file_uri[5:]
            try:
                s3.delete_file(relative_path)
                print(f"Deleted {file_uri}")
            except Exception as e:
                print(f"Failed to delete {file_uri}: {e}")
    return orphan_files


if __name__ == '__main__':
    # Configure the catalog using your S3 settings.
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
    
    # Load an existing table.
    table = catalog.load_table("om.students")
    table_location = table.location()
    
    # Collect and combine files from both metadata and data subdirectories.
    combined_files = collect_table_files(table)
    print("Combined files:")
    print(combined_files)
    
    # Extract all metadata files (snapshot, metadata log, manifest list).
    metadata_files = extract_all_metadata_files(table)
    print("\nExtracted metadata files:")
    print(metadata_files)
    
    # Identify orphan files (files in S3 data not referenced in metadata).
    orphan_files = find_orphan_files(table_location, table)
    print("\nOrphan files:")
    print(orphan_files)
    
    # Execute the delete orphan files action in dry-run mode.
    print("\nExecuting Delete Orphan Files (Dry Run):")
    delete_orphan_files(table, dry_run=True)
    
    # To actually delete the orphan files, set dry_run=False.
    # Uncomment the line below to perform actual deletion.
    # delete_orphan_files(table, dry_run=False)
