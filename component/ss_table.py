from .bloom_filter import BloomFilter
from .sparse_index import SparseIndex
from .datablock import DataBlock

import pickle

class SSTable:

    def __init__(self, filename, block_size=4):
        """Initialize with just the file name and block size."""
        self.filename = f"{filename}_sstable.pkl"
        self.bloom_filter = BloomFilter(file_name=filename)  # Bloom filter stored to disk
        self.block_size = block_size
        self.sparse_index = None  # The sparse index will be loaded when needed

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
        """Lazy load the SSTable and retrieve the value for a specific key."""
        # Load the sparse index if not already loaded

        # Use the bloom filter to check for key presence
        if not self.bloom_filter.might_contain(key):
            return None  # If bloom filter says key isn't there, avoid disk I/O

        self.load_sparse_index()
        
        # Use sparse index to find the block containing the key
        for i in range(len(self.sparse_index.index)):
            current_key, offset = self.sparse_index.index[i]

            # If key is less than current key, check previous block
            if current_key > key and i > 0:
                _, previous_offset = self.sparse_index.index[i - 1]
                block = self.get_block_by_offset(previous_offset)
                return self.search_block(block, key)

        return None  # Key not found

    def search_block(self, block, key):
        """Search through a block for the given key."""
        for k, v in block:
            if k == key:
                return v
        return None

    def might_contain(self, key):
        """Check if the key might be present using the bloom filter."""
        return self.bloom_filter.might_contain(key)
