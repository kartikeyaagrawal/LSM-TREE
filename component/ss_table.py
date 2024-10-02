from .bloom_filter import BloomFilter
from .sparse_index import SparseIndex
from .datablock import DataBlock
from ..constants import BLOCK_SIZE

import pickle
import bisect

class SSTable:

    def __init__(self, filename, block_size=BLOCK_SIZE, simple_load_file=False):
        """Initialize with just the file name and block size."""
        self.filename = f"{filename}_sstable.pkl"
        if simple_load_file==True:
            self.bloom_filter = None
        else:
            self.bloom_filter = BloomFilter(file_name=filename)  # Bloom filter stored to disk
        self.block_size = block_size
        self.sparse_index = None  # The sparse index will be loaded when needed

    def load_entire_file_into_memory(self):
        """Forcefully load the entire SSTable file into memory."""
        memory_data = None
        with open(self.filename, 'rb') as f:
            memory_data = f.read()  # Read the entire SSTable file into memory
        return memory_data
        print(f"Loaded SSTable {self.filename} into memory.")

    def save_to_disk(self, data):
        """Save the SSTable data, bloom filter, and sparse index to disk."""
        offset = 0
        sparse_index = SparseIndex(file_name=self.filename)
        with open(self.filename, 'wb') as f:
            block = DataBlock(size=self.block_size, file=f)

            for i, (key, value) in enumerate(data):
                block.add((key, value))
                self.bloom_filter.add(key)  # Add keys to Bloom filter
                # Add to sparse index when block starts
                if i % self.block_size == 0:
                    offset = f.tell()
                    sparse_index.add_index((key, offset))

            # Flush any remaining data in the block
            block.flush()

        # Save Bloom filter and sparse index to disk
        self.bloom_filter.save_to_disk()
        sparse_index.save_to_disk()
        self.sparse_index = sparse_index  # Optional: keep the sparse index in memory

    def load_sparse_index(self):
        """Lazy load the sparse index from disk when needed."""
        if self.sparse_index is None:
            self.sparse_index = SparseIndex.load_from_disk(f"{self.filename}_sparse.pkl")

    def get_block_by_offset(self, offset):
        """Retrieve a block of key-value pairs from the file using the offset."""
        with open(self.filename, 'rb') as f:
            data_block = DataBlock(size=self.block_size)
            block = data_block.read_block(f, offset)
            return block

    def get(self, key):
        """Lazy load the SSTable and retrieve the highest value lower than the specific key."""
        # Use the bloom filter to check for key presence
        if not self.bloom_filter.might_contain(key):
            return None  # If bloom filter says key isn't there, avoid disk I/O

        self.load_sparse_index()

        # Binary search on the sparse index to find the block containing the key
        low, high = 0, len(self.sparse_index.index) - 1
        block_offset = None

        while low <= high:
            mid = (low + high) // 2
            current_key, offset = self.sparse_index.index[mid]

            if current_key <= key:
                block_offset = offset  # Update the highest key lower than the key
                low = mid + 1  # Search right half
            else:
                high = mid - 1  # Search left half

        if block_offset is not None:
            # Retrieve the block and search for the key
            block = self.get_block_by_offset(block_offset)
            return self.search_block(block, key)

        return None  # Key not found

    def search_block(self, block, key):
        """Search through a block for the given key using binary search with a custom comparator."""
        
        # Define a custom comparator function
        def compare(item, key):
            # item is a tuple (k, v) from the block, key is the search key
            if item[0] == key:
                return 0
            elif item[0] < key:
                return -1
            else:
                return 1

        # Binary search implementation with the custom comparator
        low, high = 0, len(block) - 1
        while low <= high:
            mid = (low + high) // 2
            cmp_result = compare(block[mid], key)

            if cmp_result == 0:
                return block[mid][1]  # Found the key, return the corresponding value
            elif cmp_result < 0:
                low = mid + 1  # Search right half
            else:
                high = mid - 1  # Search left half
        
        return None  # Key not found


    def might_contain(self, key):
        """Check if the key might be present using the bloom filter."""
        return self.bloom_filter.might_contain(key)
