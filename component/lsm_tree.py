from .mem_table import Memtable
from .ss_table import SSTable
import time

class LSMTree:
    def __init__(self, memtable_limit=8):
        self.memtable = Memtable()
        self.sstable_filenames = []  # Only store SSTable file names
        self.memtable_limit = memtable_limit

    def insert(self, key, value):
        self.memtable.insert(key, value)
        if len(self.memtable.data) >= self.memtable_limit:
            sstable_data = self.memtable.flush()
            filename = f'{len(self.sstable_filenames)}'
            sstable = SSTable(filename)
            sstable.save_to_disk(sstable_data)  # Save the SSTable to disk
            self.sstable_filenames.append(filename)  # Only save filename
            self.memtable = Memtable()  # Reset memtable

    def get(self, key):
        # Check in the memtable first
        value = self.memtable.get(key)
        if value is not None:
            return value

        # If not found in memtable, check the SSTables
        for filename in reversed(self.sstable_filenames):  # Start from the latest
            sstable = SSTable(filename)  # Load SSTable object (no data loaded yet)
                  
            value = sstable.get(key)  # This will load the SSTable and search the key
            if value is not None:
                return value

        return None  # Key not found

