#include <iostream>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <queue>
#include <set>


class SpaceSaving {
public:
    enum DataStructure {
        HashTableOnly,
        Heap,
        SortedArray
    };

    SpaceSaving(int k, DataStructure data_structure) : k(k), total_elements(0), data_structure(data_structure) {}

    void process(const std::vector<int>& stream) {
        for (const auto& element : stream) {
            increment_or_add(element);
        }
        calculate_frequencies();
    }

    std::vector<std::pair<int, std::pair<int, float>>> get_heavy_hitters(float threshold) const {
        std::vector<std::pair<int, std::pair<int, float>>> heavy_hitters;
        for (const auto& counter : counters) {
            if (counter.second.second > threshold) {
                heavy_hitters.push_back(counter);
            }
        }
        std::sort(heavy_hitters.begin(), heavy_hitters.end(),
                  [](const auto& l, const auto& r) { return l.second.first > r.second.first; });
        return heavy_hitters;
    }

private:
    int k;  // capacity k of the histogram
    int total_elements; // total number of elements processed
    std::unordered_map<int, std::pair<int, float>> counters;  // dictionary: hash table with count and frequency
    DataStructure data_structure;

    // For heap and sorted array
    std::priority_queue<std::pair<int, int>, std::vector<std::pair<int, int>>, std::greater<>> min_heap;
    std::set<std::pair<int, int>> sorted_set;

    void increment_or_add(int element) {
        total_elements++;
        switch (data_structure) {
            case HashTableOnly:
                increment_or_add_hash_table(element);
                break;
            case Heap:
                increment_or_add_heap(element);
                break;
            case SortedArray:
                increment_or_add_sorted_array(element);
                break;
        }
    }

    void increment_or_add_hash_table(int element) {
        if (counters.find(element) != counters.end()) {
            // Increment count
            counters[element].first++;
        } else {
            // Add new element
            if (counters.size() < k) {
                counters[element] = {1, 0.0};
            } else {
                // Replace the element with the smallest count
                auto min_element = std::min_element(counters.begin(), counters.end(),
                                                    [](const auto& l, const auto& r) { return l.second.first < r.second.first; });
                int min_count = min_element->second.first;
                counters.erase(min_element);
                counters[element] = {min_count + 1, 0.0};
            }
        }
    }

    void increment_or_add_heap(int element) {
        if (counters.find(element) != counters.end()) {
            // Increment count
            counters[element].first++;
            // Update heap
            std::priority_queue<std::pair<int, int>, std::vector<std::pair<int, int>>, std::greater<>> temp_heap;
            while (!min_heap.empty()) {
                auto top = min_heap.top();
                min_heap.pop();
                if (top.second == element) {
                    top.first = counters[element].first;
                }
                temp_heap.push(top);
            }
            std::swap(min_heap, temp_heap);
        } else {
            // Add new element
            if (counters.size() < k) {
                counters[element] = {1, 0.0};
                min_heap.push({1, element});
            } else {
                // Replace smallest count
                auto min_element = min_heap.top();
                min_heap.pop();
                int min_count = min_element.first;
                counters.erase(min_element.second);
                counters[element] = {min_count + 1, 0.0};
                min_heap.push({min_count + 1, element});
            }
        }
    }

    void increment_or_add_sorted_array(int element) {
        if (counters.find(element) != counters.end()) {
            // Increment count
            int count = counters[element].first;
            sorted_set.erase({count, element});
            int new_count = count + 1;
            counters[element].first = new_count;
            sorted_set.insert({new_count, element});
        } else {
            if (counters.size() < k) {
                // Add new element
                counters[element] = {1, 0.0};
                sorted_set.insert({1, element});
            } else {
                // Replace smallest count
                auto min_element = *sorted_set.begin();
                sorted_set.erase(sorted_set.begin());
                int min_count = min_element.first;
                counters.erase(min_element.second);
                counters[element] = {min_count + 1, 0.0};
                sorted_set.insert({min_count + 1, element});
            }
        }
    }

    void calculate_frequencies() {
        for (auto& counter : counters) {
            counter.second.second = static_cast<float>(counter.second.first) / total_elements;
        }
    }

};
