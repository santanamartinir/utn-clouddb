#include <iostream>
#include <boost/asio.hpp>
#include <thread>
#include <string>
#include <utility>
#include <fstream>
#include <vector>
#include <filesystem>

using namespace std;
namespace fs = filesystem;

struct joined_row{
    uint32_t join_val;
    uint32_t row_R;
    uint32_t row_S;
};

struct tuples_data {
    std::vector<joined_row> tuples;
    int filled_rows;
};

vector<pair<string, int>> readServerConfig(const string &filename);
vector<string> get_all_files_in_directory(const string& directory_path);
string find_file_with_prefix(const vector<string>& file_names, const string& prefix);
vector<joined_row> read_data(const string& filename);
void print_raw_hex(const vector<joined_row>& v);
bool compare_by_row_S(const joined_row& a, const joined_row& b);
void calculate_receiver_and_store(vector<joined_row>& rows, uint32_t n);
vector<tuple<uint32_t, size_t, size_t>> get_first_occurrence_and_count(const vector<joined_row>& sorted_rows);
void send_data_to_all_servers(
    int my_id,
    const vector<pair<string, int>>& config,
    const vector<joined_row>& data,
    const vector<tuple<uint32_t, size_t, size_t>>& mem_locations);
void send_vec_to_server(const vector<joined_row>& pV_jR, string server_ip, string server_port, size_t offset, size_t number);
int receive_vec_from_one_server(boost::asio::io_context& io_context, int my_port, vector<joined_row>& data_buffer, size_t offset);
void receive_data_from_all_servers(boost::asio::io_context& io_context, int my_port, vector<joined_row>& data_buffer, int my_id, int n_servers);
std::vector<joined_row> inner_join(const tuples_data& r_data, const tuples_data& s_data);