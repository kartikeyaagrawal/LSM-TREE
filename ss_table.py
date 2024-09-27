from bloom_filter import BloomFilter
import pickle

class SSTable:
    def __init__(self, data, filename):
        self.data = dict(data)  # Store key-value pairs as a dictionary
        self.bloom_filter = BloomFilter()
        for key in self.data:
            self.bloom_filter.add(key)  # Add keys to Bloom filter
        self.filename = filename
        self.save_to_disk()  # Save SSTable to disk

    def save_to_disk(self):
        with open(self.filename, 'wb') as f:
            pickle.dump((self.data, self.bloom_filter.bits), f)

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