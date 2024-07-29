from collections import defaultdict

def selective_broadcast(R_partitions, S_partitions, heavy_hitters):
    print(f"Heavy hitters: {heavy_hitters}")
    print(f"Redistributing data based on heavy hitters...")
    broadcasted_tuples_R = defaultdict(list)  # Dict to save R tuples that need to be broadcasted
    kept_local_tuples_S = defaultdict(list)  # Dict to save S tuples that stay local
    
    # Process R partitions
    for partition, tuples in R_partitions.items():  # Iterate over each partition and its tuples
        for key, value in tuples:  # Iterate over each tuple in the partition
            if key in heavy_hitters:  # If the key is a heavy hitter
                for p in R_partitions:  # Redistribute the tuple
                    broadcasted_tuples_R[p].append((key, value))
            else:  # If not a heavy hitter
                broadcasted_tuples_R[partition].append((key, value))  # Keep the tuple in its original partition
    
    # Process S partitions
    for partition, tuples in S_partitions.items():  # Iterate over each partition and its tuples
        for key, value in tuples:  # Iterate over each tuple in the partition
            kept_local_tuples_S[partition].append((key, value))  # Keep the tuple in its original partition
    
    print(f"Broadcasted R partitions: {dict(broadcasted_tuples_R)}")
    print(f"Kept local S partitions: {dict(kept_local_tuples_S)}")
    return broadcasted_tuples_R, kept_local_tuples_S