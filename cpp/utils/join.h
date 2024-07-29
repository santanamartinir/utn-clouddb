#include <vector>
#include <unordered_map>

// Definition of joined_row
struct joined_row {
    int join_val;  // Value for which the join is made
};

// Definition of tuples_data
struct tuples_data {
    std::vector<joined_row> tuples;
    int filled_rows;
};

// Declaration of the inner_join function
std::vector<joined_row> inner_join(const tuples_data& r_data, const tuples_data& s_data);