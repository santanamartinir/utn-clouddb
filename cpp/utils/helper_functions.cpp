#include "helper_functions.h"
#include <iomanip>

vector<pair<string, int>> readServerConfig(const string &filename) {
    vector<pair<string, int>> serverConfig;
    ifstream file(filename);
    
    if (!file.is_open()) {
        cerr << "Could not open the file!" << endl;
        return serverConfig;
    }
    
    string ip;
    int port;
    while (file >> ip >> port) {
        serverConfig.push_back(make_pair(ip, port));
    }
    
    file.close();
    return serverConfig;
}

vector<string> get_all_files_in_directory(const string& directory_path) {
    vector<string> file_names;

    try {
        for (const auto& entry : fs::directory_iterator(directory_path)) {
            if (fs::is_regular_file(entry.status())) {
                file_names.push_back(entry.path().filename().string());
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
    const uint8_t* byte_ptr = reinterpret_cast<const uint8_t*>(v.data());
    size_t size = v.size() * sizeof(joined_row);

    for (size_t i = 0; i < size; ++i) {
        cout << hex << setw(2) << setfill('0') << static_cast<int>(byte_ptr[i]) << " ";
    }
    cout << dec << endl;
}

void calculate_receiver_and_store(vector<joined_row>& rows, uint32_t n) {
    for (auto& row : rows) {
        row.row_S = row.join_val % n + 1;
    }
}

// Comparator function to sort by row_S
bool compare_by_row_S(const joined_row& a, const joined_row& b) {
    return a.row_S < b.row_S;
}

vector<tuple<uint32_t, size_t, size_t>> get_first_occurrence_and_count(const vector<joined_row>& sorted_rows) {
    vector<tuple<uint32_t, size_t, size_t>> result;

    if (sorted_rows.empty()) return result;

    uint32_t previous_mod_value = sorted_rows[0].row_S;
    size_t start_index = 0;
    size_t count = 0;

    for (size_t i = 0; i < sorted_rows.size(); ++i) {
        const auto& row = sorted_rows[i];
        uint32_t current_mod_value = row.row_S;

        if (current_mod_value == previous_mod_value) {
            ++count;
        } else {
            result.emplace_back(previous_mod_value, start_index, count);
            previous_mod_value = current_mod_value;
            start_index = i;
            count = 1; // Reset count for the new value
        }
    }
    
    // Don't forget to add the last encountered value
    result.emplace_back(previous_mod_value, start_index, count);

    return result;
}
