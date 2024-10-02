import os
from .ss_table import SSTable

class Level:
    all_levels = []  # Class-level attribute to keep track of all levels

    def __init__(self, level, max_size):
        self.level = level
        self.max_size = max_size  # Maximum allowed size (in number of SSTables)
        self.files = []  # Files at this level
        Level.all_levels.append(self)  # Append the current instance to the class-level list

    def add_file(self, file):
        """Add a new SSTable file to the level and check for compaction."""
        self.files.append(file)
        print(f"Added file {file} to level {self.level}")

        # If the total size exceeds the limit, perform compaction
        if self.get_total_size() > self.max_size:
            new_file = self.compact()
            if new_file:
                return self.move_file_to_next_level(new_file)  # Move to the next level if needed
        return None

    def move_file_to_next_level(self, new_file):
        """Move the newly created file to the next level."""
        next_level_index = self.level + 1  # Start checking from the next level
        while next_level_index < len(Level.all_levels):
            if Level.all_levels[next_level_index].get_total_size() < Level.all_levels[next_level_index].max_size:
                Level.all_levels[next_level_index].add_file(new_file)
                return
            else:
                # Perform compaction if the current next level is full
                new_file = Level.all_levels[next_level_index].compact()
                next_level_index += 1

        # If we've gone through all levels, create a new level
        new_level = Level(next_level_index, self.max_size * 2)
        new_level.add_file(new_file)  # Add the file to the new level
        return new_level  # Return the new level for further processing

    def get_total_size(self):
        """Get the total size of SSTables in this level."""
        return len(self.files)

    def compact(self):
        """Perform compaction and merge files to the next level."""
        print(f"Compacting level {self.level}")

        # Merge all SSTables in this level into a single file
        merged_data = self.merge_sstables()
        new_sstable_file = f"level_{self.level + 1}_sstable"

        # Delete old files after compaction
        self.delete_old_files()

        return new_sstable_file

    def merge_sstables(self, next_level_files=None):
        """Merge multiple SSTable files into one based on the level."""
        
        if self.level_number == 0:
            # Special merging logic for Level 0 (merge starting from latest)
            print(f"Merging SSTables for Level 0: {self.files}")
            merged_data = self.merge_latest_sstables()
            
            # Save merged data into the next level (Level 1)
            # temp_sstable = SSTable('temp_level1_merge')
            # temp_sstable.save_to_disk(merged_data)
            
            # Add temp_sstable to the next level (Level 1)
            next_level = next_level_files or Level(1, self.max_size * 2)
            next_level = self.merge_into_next_level(merged_data, next_level)
            return next_level

    def merge_latest_sstables(self):
        """Merge the latest SSTables, keeping the most recent data."""
        merged_data = {}
        
        # Read and merge SSTables starting from the latest
        for file in reversed(self.files):  # Start with the latest files first
            sstable = SSTable(file)
            file_data = sstable.read_all_data()
            for key, value in file_data:
                if key not in merged_data:
                    merged_data[key] = value  # Add the latest data for this key
        
        # Convert the merged data dictionary to a list of tuples and return
        return sorted(merged_data.items(), key=lambda x: x[0])

    def merge_into_next_level(self, temp_sstable, next_level):
        """Merge temp SSTable with the sorted SSTables in the next level."""
        temp_data = temp_sstable  # Load the temp SSTable data
        merged_data = []
        temp_index = 0
        
        for file in next_level.files:
            sstable = SSTable(file)
            file_data = sstable.load_entire_file_into_memory()  # Read sorted data from the SSTable
            i = 0
            os.remove(file)
            # Merge temp SSTable data into the sorted SSTable file_data
            while i < len(file_data) and temp_index < len(temp_data):
                temp_key, temp_value = temp_data[temp_index]
                file_key, file_value = file_data[i]

                if temp_key < file_key:
                    # Add temp data if it comes before the file data key
                    merged_data.append((temp_key, temp_value))
                    temp_index += 1
                elif temp_key == file_key:
                    # Update the value with temp SSTable value
                    merged_data.append((temp_key, temp_value))
                    i += 1
                    temp_index += 1
                else:
                    # Add the old SSTable data if temp key hasn't caught up
                    merged_data.append((file_key, file_value))
                    i += 1

            # Append any remaining file_data
            while i < len(file_data):
                merged_data.append(file_data[i])
                i += 1

            # Save the merged result back into a new SSTable
            new_sstable = SSTable(f"merged_{file}_sstable")
            new_sstable.save_to_disk(merged_data)

            # Reset merged_data for the next file in the level
            merged_data.clear()

        # If there are remaining temp_data, merge them into a new file
        while temp_index < len(temp_data):
            merged_data.append(temp_data[temp_index])
            temp_index += 1

        if merged_data:
            final_sstable = SSTable(f"final_merged_level{next_level.level_number}")
            final_sstable.save_to_disk(merged_data)
            next_level.add_file(final_sstable.filename)

        return next_level

    def delete_old_files(self):
        """Delete old SSTable files in this level."""
        for file in self.files:
            if os.path.exists(file):
                os.remove(file)  # Remove the file from disk
                print(f"Deleted {file}")
        self.files = []  # Clear the list of files after deletion
