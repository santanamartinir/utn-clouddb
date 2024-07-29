import time
import matplotlib.pyplot as plt
import numpy as np
from heavy_hitter_detection import SpaceSaving


# Generate synthetic dataset (certain amount of unique values, size)
def generate_data(distinct_values, length=10**6):
    return np.random.randint(0, distinct_values, length)


# Update rates
def update_rate(k, data_structure, distinct_values_list):
    update_rates = []
    for distinct_values in distinct_values_list:
        stream = generate_data(distinct_values)
        saved_space = SpaceSaving(k, data_structure)
        start_time = time.time()
        saved_space.process(stream)
        end_time = time.time()
        update_rate = len(stream) / (end_time - start_time)
        update_rates.append(update_rate)
    return update_rates


# set k and distinct values like in the paper
k = 128
distinct_values_list = [2**i for i in range(1, 16)]

# Update rates for data structures
update_rates_hash_table = update_rate(k, 'hash_table_only', distinct_values_list)
update_rates_heap = update_rate(k, 'heap', distinct_values_list)
update_rates_sorted_array = update_rate(k, 'sorted_array', distinct_values_list)

# plot
plt.figure(figsize=(10, 6))
plt.plot(distinct_values_list, update_rates_hash_table, marker='o', label='hash table')
plt.plot(distinct_values_list, update_rates_heap, marker='s', label='min-heap')
plt.plot(distinct_values_list, update_rates_sorted_array, marker='^', label='sorted array')
plt.xscale('log', base=2)
plt.xticks(distinct_values_list)
plt.yscale('linear')
# plt.yscale('log', base=10)
plt.xlabel('distinct values')
plt.ylabel('updates/s')
plt.title('Single-threaded update rate, k = 128')
plt.legend()
plt.grid(True)
plt.savefig('update_rate_plot_py.png', format='png')
plt.show()
