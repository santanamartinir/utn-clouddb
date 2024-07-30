#include <iostream>
#include <unordered_map>
#include <vector>
#include "../cpp/utils/helper_functions.h"

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
}

int main() {
    test_inner_join();
    return 0;
}