from component.ss_table import SSTable
from component.lsm_tree import LSMTree

# Example usage
if __name__ == "__main__":
    lsm_tree = LSMTree(memtable_limit=8)

    # Insert some key-value pairs
    lsm_tree.insert("key0", "value0")
    lsm_tree.insert("key1", "value1")
    lsm_tree.insert("key2", "value2")
    lsm_tree.insert("key3", "value3")
    lsm_tree.insert("key4", "value4")
    lsm_tree.insert("key5", "value5")
    lsm_tree.insert("key6", "value6")
    lsm_tree.insert("key7", "value7")
    lsm_tree.insert("key8", "value8")

    # Additional key-value pairs
    lsm_tree.insert("key9", "value9")
    lsm_tree.insert("key10", "value10")
    lsm_tree.insert("key11", "value11")
    lsm_tree.insert("key12", "value12")
    lsm_tree.insert("key13", "value13")
    lsm_tree.insert("key14", "value14")
    lsm_tree.insert("key15", "value15")
    lsm_tree.insert("key16", "value16")
    lsm_tree.insert("key17", "value17")
    lsm_tree.insert("key18", "value18")
    lsm_tree.insert("key19", "value19")



    # Retrieve values
    # print(lsm_tree.get("key1"))  # Output: value1
    # print(lsm_tree.get("key2"))  # Output: value2
    # print(lsm_tree.get("key4"))  # Output: value4
    print(lsm_tree.get("key5"))  # Output: None (not found)