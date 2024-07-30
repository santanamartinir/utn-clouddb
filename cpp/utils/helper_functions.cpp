#include <iomanip>
#include <unordered_map>
#include "helper_functions.h"

vector<string> get_all_files_in_directory(const string& directory_path) {
    vector<string> file_names;

    try {
        // Iterate through the directory entries
        for (const auto& entry : fs::directory_iterator(directory_path)) {
            if (fs::is_regular_file(entry.status())) { // Check if the entry is a regular file
                file_names.push_back(entry.path().filename().string()); // Add the file name to the list
            }
        }
    } catch (const fs::filesystem_error& e) {
        cerr << "Error accessing directory: " << e.what() << endl;
    }

    return file_names;
}

// Function to find the file that starts with a given prefix
string find_file_with_prefix(const vector<string>& file_names, const string& prefix) {
    for (const auto& file_name : file_names) {
        if (file_name.rfind(prefix, 0) == 0) { // Check if the file name starts with the prefix
            return file_name;
        }
    }
    cerr << "Error: No file found with prefix: " << prefix << endl;
    return ""; // Return an empty string if no file matches the prefix
}

vector<joined_row> read_data(const string& filename) {
    vector<joined_row> data;
    ifstream file(filename);

    if (!file.is_open()) {
        throw runtime_error("Could not open file: " + filename);
    }

    uint32_t val, row_idx;
    while (file >> val >> row_idx) {
        joined_row row = {val, row_idx, 0};
        data.push_back(row);
    }

    file.close();
    return data;
}

void print_raw_hex(const vector<joined_row>& v) {
    const uint8_t* byte_ptr = reinterpret_cast<const uint8_t*>(v.data()); // Get a pointer to the data
    size_t size = v.size() * sizeof(joined_row);

    for (size_t i = 0; i < size; ++i) {
        cout << hex << setw(2) << setfill('0') << static_cast<int>(byte_ptr[i]) << " ";
    }
    cout << dec << endl; // Print a newline and reset the format to decimal
}

void calculate_receiver_and_store(vector<joined_row>& rows, uint32_t n) {
    for (auto& row : rows) {
        row.row_S = row.join_val % n + 1; // Compute the row_S value
    }
}

// Comparator function to sort by row_S
bool compare_by_row_S(const joined_row& a, const joined_row& b) {
    return a.row_S < b.row_S; // Compare based on the row_S value
}

// Function to get the first occurrence and count of each unique join_val in a sorted vector of joined_row structures
vector<tuple<uint32_t, size_t, size_t>> get_first_occurrence_and_count(const vector<joined_row>& sorted_rows) {
    vector<tuple<uint32_t, size_t, size_t>> result;

    if (sorted_rows.empty()) return result;

    uint32_t previous_mod_value = sorted_rows[0].row_S;
    size_t start_index = 0;
    size_t count = 0;

    // Iterate through the sorted rows to count occurrences
    for (size_t i = 0; i < sorted_rows.size(); ++i) {
        const auto& row = sorted_rows[i];
        uint32_t current_mod_value = row.row_S;

        if (current_mod_value == previous_mod_value) { // If the value is the same as the previous one
            ++count; // Increment the count
        } else {
            result.emplace_back(previous_mod_value, start_index, count);
            previous_mod_value = current_mod_value;
            start_index = i;
            count = 1; // Reset count for the new value
        }
    }
    
    // Add the last encountered value
    result.emplace_back(previous_mod_value, start_index, count);

    return result;
}

// Implementation of the inner_join function
std::vector<joined_row> inner_join(const tuples_data& r_data, const tuples_data& s_data) {
    std::vector<joined_row> result;
    std::unordered_map<int, std::vector<joined_row>> hashTable; // Map to store the rows of r_data using the join value as key

    int r_size = r_data.filled_rows;
    int s_size = s_data.filled_rows;
    
    // Build the table from the data in r_data
    for (int i = 0; i < r_size; ++i) {
        const joined_row& row = r_data.tuples[i];
        hashTable[row.join_val].push_back(row); // Insert row into hash table with join_val as key
    }

    // Process s_data and perform join
    for (int i = 0; i < s_size; ++i) {
        const joined_row& row = s_data.tuples[i]; // Get the current row
        auto it = hashTable.find(row.join_val); // Find the join_val in the hash table
        // If the join_val is found in the hash table
        if (it != hashTable.end()) {
            for (const auto& r_row : it->second) { // Iterate over the matching rows in r_data
                joined_row joined;
                joined.join_val = row.join_val;
                joined.row_R = r_row.row_R;
                joined.row_S = row.row_S;
                result.push_back(joined); // Add the joined row to the result vector
            }
        }
    }

    return result;
}