class Memtable:
    def __init__(self):
        self.data = {}  # Use a dictionary to store key-value pairs

    def insert(self, key, value):
        self.data[key] = value

    def get(self, key):
        return self.data.get(key)

    def flush(self):
        return sorted(self.data.items())