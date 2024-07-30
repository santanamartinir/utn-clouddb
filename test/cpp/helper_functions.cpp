#include "helper_functions.h"
#include <iomanip>

using boost::asio::ip::tcp;

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

void send_data_to_all_servers(
    int my_id,
    const vector<pair<string, int>>& config,
    const vector<joined_row>& data,
    const vector<tuple<uint32_t, size_t, size_t>>& mem_locations) {

    // Check that the number of servers matches the number of memory locations
    assert(config.size() == mem_locations.size());

    // Iterate over all servers
    for (size_t idx_serv = 0; idx_serv < config.size(); ++idx_serv) {

        // Skip sending data to the server if my_id matches idx_serv
        if (my_id == static_cast<int>(idx_serv + 1)) {
            continue;
        }

        // Get the server's IP and port from the config
        const auto& server_info = config[idx_serv];
        const string& server_ip = server_info.first;
        const int server_port = server_info.second;

        // Get the offset and count from mem_locations
        const auto& mem_info = mem_locations[idx_serv];
        const size_t offset = std::get<1>(mem_info);
        const size_t number = std::get<2>(mem_info);

        // Print server info for debugging
        cout << "Sending data to server: " << server_ip << ":" << server_port << endl;
        cout << "Offset: " << offset << ", Number: " << number << endl;

        // Send the chunk of data to the server
        send_vec_to_server(data, server_ip, to_string(server_port), offset, number);
    }
}

void send_vec_to_server(const vector<joined_row>& pV_jR, string server_ip, string server_port, size_t offset, size_t number) {
    // Ensure the offset and number are within the bounds of the vector
    if (offset >= pV_jR.size()) {
        throw std::out_of_range("Offset is out of range.");
    }
    if (offset + number > pV_jR.size()) {
        number = pV_jR.size() - offset;  // Adjust number to send till the end of the vector
    }

    boost::asio::io_context io_context;
    tcp::resolver resolver(io_context);
    tcp::resolver::results_type endpoints = resolver.resolve(server_ip, server_port);
    tcp::socket socket(io_context);
    boost::asio::connect(socket, endpoints);

    // Calculate the number of bytes to send
    size_t bytes_to_send = number * sizeof(joined_row);

    // Send the chunk of the vector
    boost::asio::write(socket, boost::asio::buffer(pV_jR.data() + offset, bytes_to_send));
}

int receive_vec_from_one_server(boost::asio::io_context& io_context, int my_port, vector<joined_row>& data_buffer, size_t offset) {
    tcp::acceptor acceptor(io_context, tcp::endpoint(tcp::v4(), my_port));
    tcp::socket socket(io_context);
    acceptor.accept(socket);

    boost::system::error_code error;

    // Calculate the starting position in the buffer
    char* start_pos = reinterpret_cast<char*>(data_buffer.data()) + offset * sizeof(joined_row);

    // Calculate the remaining space in the buffer from the offset
    size_t remaining_space = (data_buffer.size() * sizeof(joined_row)) - offset * sizeof(joined_row);

    cout << "In rec. vec. Offset: " << offset << " remaining space: " << remaining_space << endl;

    // Ensure there is space to read into
    if (remaining_space == 0) {
        throw std::runtime_error("No space left in the buffer to read data.");
    }

    size_t length = socket.read_some(boost::asio::buffer(start_pos, remaining_space), error);

    if (error == boost::asio::error::eof) {
        cout << "Connection closed cleanly by peer.\n";
    } else if (error) {
        throw boost::system::system_error(error); // Some other error.
    }

    cout << "Received buffer w. length: " << length << endl;
    int n_rows = length / sizeof(joined_row);
    for (int i = 0; i < n_rows; i++) {
        cout << data_buffer[offset + i].join_val << " " << data_buffer[offset + i].row_R << " " << data_buffer[offset + i].row_S << endl;
    }

    return length;
}

void receive_data_from_all_servers(boost::asio::io_context& io_context, int my_port, vector<joined_row>& data_buffer, int my_id, int n_servers) {
    int offset = 0;
    for (int i = 0; i < n_servers; ++i) {
        if (i + 1 != my_id) {  // Skip own server
            cout << "Receiving data from server " << i + 1 << endl;
            cout << "Offset " << offset << endl;
            offset += receive_vec_from_one_server(io_context, my_port, data_buffer, offset);
            cout << "Offset " << offset << endl;

        }
    }
}

// Implementation of the inner_join function
std::vector<joined_row> inner_join(const tuples_data& r_data, const tuples_data& s_data) {
    std::vector<joined_row> result;
    std::unordered_map<int, std::vector<joined_row>> hashTable;  // Map to store the rows of r_data using the join value as key

    int r_size = r_data.filled_rows;
    int s_size = s_data.filled_rows;
    
    // Build the table from the data in r_data
    for (int i = 0; i < r_size; ++i) {
        const joined_row& row = r_data.tuples[i];
        hashTable[row.join_val].push_back(row);  // Insert row into hash table with join_val as key
    }

    // Process s_data and perform join
    for (int i = 0; i < s_size; ++i) {
        const joined_row& row = s_data.tuples[i];
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