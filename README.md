# LSM Tree Implementation with Skip List Optimization

## Description

This project implements a Log-Structured Merge-Tree (LSM Tree) data structure, a highly efficient storage engine commonly used in key-value stores and databases. The LSM Tree leverages a tiered architecture to achieve high write performance and efficient storage management.

## Key Features

- **Skip List Optimization**: The implementation incorporates skip lists as an index structure, significantly improving search performance and reducing the number of disk I/O operations.
- **Tiered Architecture**: The LSM Tree is organized into multiple levels, with each level holding data in sorted order. This enables efficient compaction and query processing.
- **Compaction**: The implementation includes a robust compaction mechanism to merge data from multiple levels, reducing fragmentation and improving query efficiency.
- **Write Amplification**: The LSM Tree is designed to minimize write amplification, ensuring that data is written to disk as few times as possible.
- **Persistence**: The implementation persists data to disk, ensuring data durability even in the event of system failures.










name changes to file 
discitoary to store datablock 


Do you load the whole file in the disk ? when merging ???

Adding the compaction form level1 to level2
Changing the dictioniary to skipliadt 
while not loading via extend data block bit like get_next()
adding the cache system when using get and flushing cache when not in use 