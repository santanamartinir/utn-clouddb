#include <iostream>
#include <unordered_map>
#include <vector>
#include "join.h"

// Implementation of the inner_join function
std::vector<joined_row> inner_join(const tuples_data& r_data, const tuples_data& s_data) {
    std::vector<joined_row> result;
    std::unordered_map<int, std::vector<joined_row>> hashTable;  // Map to store the rows of r_data using the union value as key

    // Build the table from the data in r_data
    for (const auto& row : r_data.tuples) {
        hashTable[row.join_val].push_back(row);  // Insert row into hash table with join_val as key
    }

    // Process s_data and perform join
    for (const auto& row : s_data.tuples) {
        auto it = hashTable.find(row.join_val);
        // If the join_val is found in the hash table
        if (it != hashTable.end()) {
            for (const auto& r_row : it->second) { 
                joined_row joined;
                joined.join_val = row.join_val;
                result.push_back(joined);
            }
        }
    }

    return result;
}

// l.35 - 63, created by ChatGPT
// Accessed 29/07/2024
// Test inner_join fucntion
void test_inner_join() {
    // Create example data for r_data
    tuples_data r_data;
    r_data.tuples = {
        {1}, {2}, {3}, {4}
    };
    r_data.filled_rows = r_data.tuples.size();

    // Create example data for s_data
    tuples_data s_data;
    s_data.tuples = {
        {3}, {4}, {5}, {6}
    };
    s_data.filled_rows = s_data.tuples.size();

    // Call inner_join
    std::vector<joined_row> result = inner_join(r_data, s_data);

    // Print results
    std::cout << "Result of inner join:" << std::endl;
    for (const auto& row : result) {
        std::cout << "join_val: " << row.join_val << std::endl;
    }
}

int main() {
    test_inner_join();
    return 0;
}