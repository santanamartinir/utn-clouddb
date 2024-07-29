#include <iostream>
#include <unordered_map>
#include <vector>
#include "helper_functions.h"

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
                joined.row_R = r_row.row_R;
                joined.row_S = row.row_S;
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
    // Example usage of the inner_join function
    tuples_data r_data = { 
        {{1, 10, 0}, {2, 20, 0}, {3, 30, 0}},  // Tuples for r_data
        3
    };

    tuples_data s_data = { 
        {{1, 0, 100}, {2, 0, 200}, {4, 0, 400}},  // Tuples for s_data
        3
    };

    std::vector<joined_row> result = inner_join(r_data, s_data);

    // Output the results
    for (const auto& row : result) {
        std::cout << "join_val: " << row.join_val
                  << ", row_R: " << row.row_R
                  << ", row_S: " << row.row_S
                  << std::endl;
    }

    return 0;
}

int main() {
    test_inner_join();
    return 0;
}