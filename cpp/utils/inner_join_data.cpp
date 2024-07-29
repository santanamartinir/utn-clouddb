#include <iostream>
#include <unordered_map>
#include <vector>
#include "helper_functions.h"

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