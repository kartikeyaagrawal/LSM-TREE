import pickle

from .bloom_filter import BloomFilter


class SparseIndex:

    def __init__(self, file_name) -> None:
        self.index = []
        self.file_name = file_name

    def add_index(self, key_offset_pair):
        self.index.append(key_offset_pair)

    @staticmethod
    def load_from_disk(filename):
        with open(filename, 'rb') as f:
            array = pickle.load(f)
            sstable = SparseIndex()
            return sstable
  
    def save_to_disk(self):
        with open(self.file_name, 'wb') as f:
            pickle.dump(self.index, f)



class SSTable:
    def __init__(self, data, filename, block_size = 4):
        self.data = dict(data)  # Store key-value pairs as a dictionary
        self.bloom_filter = BloomFilter()
        for key in self.data:
            self.bloom_filter.add(key)  # Add keys to Bloom filter
        self.filename = filename
        self.block_size = block_size
        self.save_to_disk()  # Save SSTable to disk

    def save_to_disk(self):
        offset = 0 
        sparse_index = SparseIndex()
        with open(self.filename, 'wb') as f:
            for i , (key, value) in self.data.items():
                pickle.dump((key, value), f)
                if i%self.block_size ==0:
                    offset = f.tell()
                    sparse_index.add_index((key,offset))
        
        sparse_index.save_to_disk()

    @staticmethod
    def load_from_disk(filename):
        with open(filename, 'rb') as f:
            data, bloom_bits = pickle.load(f)
            sstable = SSTable(data, filename)
            sstable.bloom_filter.bits = bloom_bits
            return sstable

    def get(self, key):
        return self.data.get(key)

    def might_contain(self, key):
        return self.bloom_filter.might_contain(key)