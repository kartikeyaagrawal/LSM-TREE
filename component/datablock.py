import pickle

class DataBlock:

    def __init__(self, size=4, file=None) -> None:
        self.block = []
        self.block_size = size
        self.file = file  # File object to write the block

    def add(self, key_value_pair):
        """Add a key-value pair to the block and flush when block size is reached."""
        self.block.append(key_value_pair)
        if len(self.block) == self.block_size:
            self.flush()

    def flush(self):
        """Flush the block to disk."""
        if self.file:
            # Dump the entire block (list of key-value pairs) in one go
            pickle.dump(self.block, self.file)
            self.block.clear()  # Clear the block after flushing

    def read_block(self, file, offset):
        """Read a block from the given offset."""
        file.seek(offset)  # Move the file pointer to the offset
        try:
            block = pickle.load(file)  # Load the entire block (list of key-value pairs)
        except EOFError:
            block = []  # Return an empty block if end of file is reached
        return block
