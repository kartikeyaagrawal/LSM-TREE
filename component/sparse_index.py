import pickle
class SparseIndex:

    def __init__(self, file_name, data = []) -> None:
        self.index: list = data
        self.file_name = f"{file_name}_sparse.pkl"

    def add_index(self, key_offset_pair):
        self.index.append(key_offset_pair)

    @staticmethod
    def load_from_disk(filename):
        with open(filename, 'rb') as f:
            array = pickle.load(f)
            sstable = SparseIndex(data=array, file_name=filename)
            return sstable

    def save_to_disk(self):
        with open(self.file_name, 'wb') as f:
            pickle.dump(self.index, f)