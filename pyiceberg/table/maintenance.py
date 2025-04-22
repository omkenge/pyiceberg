from dataclasses import dataclass
from typing import Set, Optional
from pyiceberg.table import Table
from pyiceberg.io import FileIO
from pyiceberg.io.fs import FileSystem, FileType, FileSelector

@dataclass
class OrphanFileCleanup:
    """Maintenance operation to identify and remove orphan data files."""
    
    table: Table
    dry_run: bool = True
    
    def _list_files(self, subdir: str) -> Set[str]:
        """List files from a specific subdirectory using table's FileIO."""
        base = self.table.location().rstrip('/')
        location = f"{base}/{subdir}"
        fs = self.table.io.new_input(location)
        selector = FileSelector(location, recursive=True)
        return {
            file_info.path 
            for file_info in fs.get_file_info(selector)
            if file_info.type == FileType.FILE
        }

    def _get_metadata_references(self) -> Set[str]:
        """Collect all files referenced in table metadata."""
        references = set()
        
        # Snapshot files
        if files := self.table.inspect.files():
            references.update(files.column("file_path").to_pylist())
        
        # Metadata logs
        if logs := self.table.inspect.metadata_log_entries():
            references.update(logs.column("file").to_pylist())
        
        # Manifest lists
        if snaps := self.table.inspect.snapshots():
            references.update(snaps.column("manifest_list").to_pylist())
        
        return references

    def execute(self) -> Set[str]:
        """Execute the orphan file cleanup operation."""
        data_files = self._list_files("data")
        metadata_refs = self._get_metadata_references()
        orphans = data_files - metadata_refs

        if not self.dry_run:
            io = self.table.io
            for file_path in orphans:
                io.delete(file_path)

        return orphans