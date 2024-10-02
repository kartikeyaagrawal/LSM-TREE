from .mem_table import Memtable
from .ss_table import SSTable
from .level import Level

class LSMTree:
    def __init__(self, memtable_limit=8, level_max_size=4):
        self.memtable = Memtable()
        self.memtable_limit = memtable_limit
        self.levels = [Level(0, level_max_size)]  # Initialize the first level with a max size

    def insert(self, key, value):
        self.memtable.insert(key, value)
        if len(self.memtable.data) >= self.memtable_limit:
            sstable_data = self.memtable.flush()
            filename = f'level_0_sstable_{len(Level.all_levels[0].files)}'
            sstable = SSTable(filename)
            sstable.save_to_disk(sstable_data)
            self.memtable = Memtable()  # Reset the memtable
            
            # Now handle adding the SSTable to the level
            new_file = Level.all_levels[0].add_file(filename)
            if new_file:
                self.levels.append(new_file)  # Add the new level if created


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

