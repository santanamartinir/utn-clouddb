import matplotlib.pyplot as plt

# Read data from the file
with open("update_rates.txt", "r") as file:
    lines = file.readlines()

# Parse the data
header = lines[0].strip().split()
data = [list(map(float, line.strip().split())) for line in lines[1:]]

distinct_values_list = [row[0] for row in data]
update_rates_hash_table = [row[1] for row in data]
update_rates_heap = [row[2] for row in data]
update_rates_sorted_array = [row[3] for row in data]

# Plot the data
plt.figure(figsize=(10, 6))
plt.plot(distinct_values_list, update_rates_hash_table, marker='o', label='hash table')
plt.plot(distinct_values_list, update_rates_heap, marker='s', label='min-heap')
plt.plot(distinct_values_list, update_rates_sorted_array, marker='^', label='sorted array')
plt.xscale('log', base=2)
plt.xticks(distinct_values_list)
plt.yscale('log', base=10)
plt.xlabel('distinct values')
plt.ylabel('updates/s')
plt.title('Single-threaded update rate, k = 128')
plt.legend()
plt.grid(True)
plt.savefig('update_rate_cpp.png', format='png')
plt.show()
