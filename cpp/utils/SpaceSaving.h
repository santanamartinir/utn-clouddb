#include <iostream>
#include <unordered_map>
#include <vector>
#include <algorithm>

class SpaceSaving {
public:
    SpaceSaving(int k) : k(k), total_elements(0) {}

    void increment_or_add(int element) {
        total_elements++;
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

    void calculate_frequencies() {
        for (auto& counter : counters) {
            counter.second.second = static_cast<float>(counter.second.first) / total_elements;
        }
    }
};