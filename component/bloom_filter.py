import pickle
from bitarray import bitarray
import mmh3  # MurmurHash for better hash performance

class BloomFilter:
    def __init__(self, file_name, size=1000, hash_count=5):
        self.size = size
        self.hash_count = hash_count
        self.bits = bitarray(size)
        self.bits.setall(0)
        self.file_name = f"{file_name}_bloom_filter.pkl"
        self.loaded = False  # Flag to check if the filter is loaded

    def add(self, key):
        """Add a key to the Bloom filter."""
        for i in range(self.hash_count):
            index = mmh3.hash(key, i) % self.size
            self.bits[index] = 1

    def save_to_disk(self):
        """Save the Bloom filter to disk."""
        with open(self.file_name, 'wb') as f:
            pickle.dump(self.bits, f)

    def load_from_disk(self):
        """Lazy load the Bloom filter from disk."""
        if not self.loaded:
            with open(self.file_name, 'rb') as f:
                self.bits = pickle.load(f)
            self.loaded = True  # Set the flag once loaded

    def might_contain(self, key):
        """Check if the Bloom filter might contain the key."""
        self.load_from_disk()  # Load the filter only when needed
        for i in range(self.hash_count):
            index = mmh3.hash(key, i) % self.size
            if not self.bits[index]:
                return False
        return True
