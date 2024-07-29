import os

import heavy_hitter_detection as hh
import adaptive_redistribution as adr

# l. 8- 14, https://stackoverflow.com/questions/22302345/reading-a-file-in-python-for-text
# Accesed on 28/07/2024 
def read_data(file_path):
    data = []
    try:
        with open(file_path, 'r') as file:
            for line in file:
                key, value = line.strip().split()  # Split line into key and value
                data.append((int(key), int(value)))  # Append as a tuple
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
    return data

def load_partitions(base_path, prefix, num_partitions):
    partitions = {}
    for i in range(num_partitions):
        file_path = os.path.join(base_path, f"{i+1}_{prefix}.txt")
        print(f"Loading partition from file {file_path}...")
        partitions[i] = read_data(file_path)
    print(f"Loaded {num_partitions} partitions.")
    return partitions

def perform_join(R_partitions, S_partitions):
    print("Performing join operation...")
    result = []
    # Iterate over each partition of R
    for r_partition in R_partitions.values():
        # Iterate over each partition of S
        for s_partition in S_partitions.values():
            # Perform the join operation:
            # - For each tuple (r_key, _) in the current R partition
            # - For each tuple (s_key, s_value) in the current S partition
            # - If the keys (r_key and s_key) match, create a tuple (r_key, s_value)
            # - Collect all such tuples in join_result
            join_result = [(r_key, s_value) for r_key, _ in r_partition for s_key, s_value in s_partition if r_key == s_key]
            # Extend the result list with the tuples found in join_result
            result.extend(join_result)
            print(f"Join result for partitions: {join_result}")
    result = list(set(result))  # Avoid duplicates
    print(f"Join operation complete. Number of results: {len(result)}")
    return result

def flow_join(R_base_path, S_base_path, num_partitions, k, skew_threshold, data_structure):
    print("Starting Flow-Join...")

    print("Loading partitions with prefix 'R_16'...")
    R_partitions = load_partitions(R_base_path, 'R_16', num_partitions)
    
    print("Loading partitions with prefix 'S_zipf_9_1p25_16_256'...")
    S_partitions = load_partitions(S_base_path, 'S_zipf_9_1p25_16_256', num_partitions)

    # Initialize SpaceSaving for R and S partitions
    print("Estimating histograms for R partitions...")
    R_histograms = {}
    R_ss = hh.SpaceSaving(k, data_structure)
    for i, partition in R_partitions.items():
        R_ss.process([key for key, _ in partition])
        R_histograms[i] = R_ss.get_heavy_hitters()
    
    print("Estimating histograms for S partitions...")
    S_histograms = {}
    S_ss = hh.SpaceSaving(k, data_structure)
    for i, partition in S_partitions.items():
        S_ss.process([key for key, _ in partition])
        S_histograms[i] = S_ss.get_heavy_hitters()

    # Detect heavy hitters
    print("Detecting heavy hitters for R partitions...")
    heavy_hitters_R = set()
    for hitters in R_histograms.values():
        heavy_hitters_R.update([key for key, _ in hitters if _ >= skew_threshold])

    print("Detecting heavy hitters for S partitions...")
    heavy_hitters_S = set()
    for hitters in S_histograms.values():
        heavy_hitters_S.update([key for key, _ in hitters if _ >= skew_threshold])

    # Redistribute data based on heavy hitters
    broadcasted_R, kept_local_S = adr.selective_broadcast(R_partitions, S_partitions, heavy_hitters_R)

    # Perform join operation
    result = list(set(perform_join(broadcasted_R, kept_local_S)))  # Avoid duplicates
    print("Join Result:", result)

    print("Flow-Join complete.")

# Main
R_base_path = "C:/Users/irene/Documents/UTN/CloudDB/utn-clouddb/cpp/bin/R_16"
S_base_path = "C:/Users/irene/Documents/UTN/CloudDB/utn-clouddb/cpp/bin/S_zipf_9_1p25_16_256"

num_partitions = 4
k = 3
skew_threshold = 3
data_structure = "hash_table_only"  # Choose 'hash_table_only', 'heap', or 'sorted_array'

flow_join(R_base_path, S_base_path, num_partitions, k, skew_threshold, data_structure)