import heapq
import numpy as np

# Using the *space saving algorithm*
# Data structure: hash table, hash table with heap, hash table with sorted array
# references:
# Rödiger W. et al, Flow-Join: Adaptive Skew Handling for Distributed Joins over High-Speed Networks
# Metwally A. et al, Efficient Computation of Frequent and Top-k Elements in Data Streams


class SpaceSaving:
    def __init__(self, k, data_structure):
        self.k = k  # capacity k of the histogram
        self.counters = {}  # dictionary: hash table
        self.data_structure = data_structure

        # three different data structures
        if data_structure == 'heap':
            self.heap = []
        elif data_structure == 'sorted_array':
            self.sorted_list = []
        elif data_structure != 'hash_table_only':
            raise ValueError('Invalid data structure! Choose hash_table_only, heap or sorted_array.')

    # * Increment or Add *
    # Increment count of existing element or add new element
    # If number of elements exceeds k (max. capacity),
    # replace the element with smallest count

    def increment_or_add_hash_table(self, element):
        # hash table only

        if element in self.counters:
            # Increment count
            self.counters[element] += 1
        else:
            # Add new element
            if len(self.counters) < self.k:
                self.counters[element] = 1
            else:
                # Replace smallest count
                min_element = min(self.counters, key=self.counters.get)
                min_count = self.counters[min_element]
                del self.counters[min_element]
                self.counters[element] = min_count + 1

    def increment_or_add_hash_heap(self, element):
        # hash table + heap

        if element in self.counters:
            # Increment count
            self.counters[element] += 1
            # update heap
            for i in range(len(self.heap)):
                if self.heap[i][1] == element:
                    self.heap[i] = (self.counters[element], element)
                    heapq.heapify(self.heap)
                    break
        else:
            # Add new element
            if len(self.counters) < self.k:
                self.counters[element] = 1
                heapq.heappush(self.heap, (1, element))
            else:
                # Replace smallest count
                min_count, min_element = heapq.heappop(self.heap)
                del self.counters[min_element]
                self.counters[element] = min_count + 1
                heapq.heappush(self.heap, (min_count + 1, element))

    def increment_or_add_hash_sorted_arr(self, element):
        # hash table + sorted array

        if element in self.counters:
            # Increment count
            count = self.counters[element]
            self.sorted_list.remove((count, element))
            new_count = count + 1
            self.counters[element] = new_count
            self.insert_sorted((new_count, element))
        else:
            if len(self.counters) < self.k:
                # Add new element
                new_count = 1
                self.counters[element] = new_count
                self.insert_sorted((new_count, element))
            else:
                # Replace smallest count
                min_count, min_element = self.sorted_list.pop(0)
                del self.counters[min_element]
                new_min_count = min_count + 1
                self.counters[element] = new_min_count
                self.insert_sorted((new_min_count, element))

    def insert_sorted(self, item):
        # Insert item to correct position in sorted list
        count, element = item
        inserted = False
        for i in range(len(self.sorted_list)):
            if self.sorted_list[i][0] > count:
                self.sorted_list.insert(i, item)
                inserted = True
                break
        if not inserted:
            self.sorted_list.append(item)

    def process(self, stream):
        # Process stream of elements
        for element in stream:
            if self.data_structure == 'hash_table_only':
                self.increment_or_add_hash_table(element)
            elif self.data_structure == 'heap':
                self.increment_or_add_hash_heap(element)
            elif self.data_structure == 'sorted_array':
                self.increment_or_add_hash_sorted_arr(element)

    def get_heavy_hitters(self):
        # Get heavy hitters sorted desc. by approximate counts
        if self.data_structure == 'hash_table_only':
            return sorted(self.counters.items(), key=lambda x: x[1], reverse=True)
        elif self.data_structure == 'heap':
            return sorted([(element, self.counters[element]) for count, element in self.heap], key=lambda x: x[1], reverse=True)
        elif self.data_structure == 'sorted_array':
            return sorted([(element, count) for count, element in self.sorted_list], key=lambda x: x[1], reverse=True)


# Example usage

# Random data
rand = [1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 7, 8, 85, 5, 4, 3, 2,
        3, 3, 6, 3, 2, 2, 1, 1, 1]

# TODO: how to set good size for capacity of histogram?
k = 3

saved_space = SpaceSaving(k, 'sorted_array')
saved_space.process(rand)
heavy_hitters = saved_space.get_heavy_hitters()
print("Heavy Hitters Random:", heavy_hitters)

# Zipf distributed data
zipf_data = np.random.zipf(4.0, 20000)
zipf_data = [int(i) for i in zipf_data]

saved_space2 = SpaceSaving(k, 'hash_table_only')
saved_space2.process(zipf_data)
heavy_hitters2 = saved_space2.get_heavy_hitters()
print("Heavy Hitters Zipf, hash table:", heavy_hitters2)

saved_space3 = SpaceSaving(k, 'heap')
saved_space3.process(zipf_data)
heavy_hitters3 = saved_space3.get_heavy_hitters()
print("Heavy Hitters Zipf, hash table heap:", heavy_hitters3)

saved_space4 = SpaceSaving(k, 'sorted_array')
saved_space4.process(zipf_data)
heavy_hitters4 = saved_space4.get_heavy_hitters()
print("Heavy Hitters Zipf, hash table sorted array:", heavy_hitters4)
