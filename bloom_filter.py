from bitarray import bitarray
import mmh3  # MurmurHash for better hash performance

class BloomFilter:
    def __init__(self, size=1000, hash_count=5):
        self.size = size
        self.hash_count = hash_count
        self.bits = bitarray(size)
        self.bits.setall(0)

    def add(self, key):
        for i in range(self.hash_count):
            index = mmh3.hash(key, i) % self.size
            self.bits[index] = 1

    def might_contain(self, key):
        for i in range(self.hash_count):
            index = mmh3.hash(key, i) % self.size
            if not self.bits[index]:
                return False
        return True
