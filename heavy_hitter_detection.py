import numpy as np

# Using the *space saving algorithm*
# Data structure: sorted array
# references:
# Rödiger W. et al, Flow-Join: Adaptive Skew Handling for Distributed Joins over High-Speed Networks
# Metwally A. et al, Efficient Computation of Frequent and Top-k Elements in Data Streams


class SpaceSaving:
    def __init__(self, k):
        self.k = k  # capacity k of the histogram
        self.counters = {}  # dictionary
        self.sorted_list = []

    def increment_or_add(self, element):
        # Increment count of existing element or add new element
        # If number of elements exceeds k (max. capacity), replace the element with smallest count
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
            self.increment_or_add(element)

    def get_heavy_hitters(self):
        # Get heavy hitters sorted desc. by approximate counts
        heavy_hitters = [(element, self.counters[element]) for count,
                         element in self.sorted_list]
        return sorted(heavy_hitters, key=lambda x: x[1], reverse=True)


# Example usage

# Random data
rand = [1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 7, 8, 85, 5, 4, 3, 2,
        3, 3, 6, 3, 2, 2, 1, 1, 1]

# TODO: how to set good size for capacity of histogram?
k = 3

saved_space = SpaceSaving(k)
saved_space.process(rand)
heavy_hitters = saved_space.get_heavy_hitters()
print("Heavy Hitters Random:", heavy_hitters)

# Zipf distributed data
zipf_data = np.random.zipf(4.0, 20000)
zipf_data = [int(i) for i in zipf_data]

saved_space2 = SpaceSaving(k)
saved_space2.process(zipf_data)
heavy_hitters2 = saved_space2.get_heavy_hitters()
print("Heavy Hitters Zipf:", heavy_hitters2)
