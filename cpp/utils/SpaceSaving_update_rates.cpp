#include "SpaceSaving.h"
#include <chrono>
#include <fstream>

// Generate synthetic dataset (certain amount of unique values, size)
std::vector<int> generate_data(int distinct_values, int length) {
    std::vector<int> data(length);
    for (int i = 0; i < length; ++i) {
        data[i] = rand() % distinct_values;
    }
    return data;
}

// Update rates
std::vector<double> update_rate(int k, SpaceSaving::DataStructure data_structure, const std::vector<int>& distinct_values_list) {
    std::vector<double> update_rates;
    for (int distinct_values : distinct_values_list) {
        auto stream = generate_data(distinct_values, 1000000);
        SpaceSaving space_saving(k, data_structure);
        auto start_time = std::chrono::high_resolution_clock::now();
        space_saving.process(stream);
        auto end_time = std::chrono::high_resolution_clock::now();
        std::chrono::duration<double> elapsed = end_time - start_time;
        double update_rate = stream.size() / elapsed.count();
        update_rates.push_back(update_rate);
    }
    return update_rates;
}


int main() {

    // set k and distinct values like in the paper
    int k = 128;
    std::vector<int> distinct_values_list;
    for (int i = 1; i <= 15; ++i) {
        distinct_values_list.push_back(1 << i);
    }

    // Update rates for data structures
    auto update_rates_hash_table = update_rate(k, SpaceSaving::HashTableOnly, distinct_values_list);
    auto update_rates_heap = update_rate(k, SpaceSaving::Heap, distinct_values_list);
    auto update_rates_sorted_array = update_rate(k, SpaceSaving::SortedArray, distinct_values_list);

    // Output results
    std::ofstream output_file("../../python/update_rates.txt");
    output_file << "distinct_values hash_table heap sorted_array\n";
    for (size_t i = 0; i < distinct_values_list.size(); ++i) {
        output_file << distinct_values_list[i] << " " << update_rates_hash_table[i] << " " << update_rates_heap[i] << " " << update_rates_sorted_array[i] << "\n";
    }
    output_file.close();

    return 0;
}
