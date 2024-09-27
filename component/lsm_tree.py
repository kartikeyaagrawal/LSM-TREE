from mem_table import Memtable
from ss_table import SSTable

class LSMTree:
    def __init__(self, memtable_limit=3):
        self.memtable = Memtable()
        self.sstables = []
        self.memtable_limit = memtable_limit

    def insert(self, key, value):
        self.memtable.insert(key, value)
        if len(self.memtable.data) >= self.memtable_limit:
            sstable_data = self.memtable.flush()
            filename = f'sstable_{len(self.sstables)}.pkl'
            self.sstables.append(SSTable(sstable_data, filename))
            self.memtable = Memtable()  # Reset memtable

    def get(self, key):
        # Check in the memtable first
        value = self.memtable.get(key)
        if value is not None:
            return value
        
        # If not found in memtable, check the SSTables
        for sstable in reversed(self.sstables):  # Start from the latest
            if sstable.might_contain(key):  # Use Bloom filter to check presence
                value = sstable.get(key)
                if value is not None:
                    return value
        
        return None  # Key not found
