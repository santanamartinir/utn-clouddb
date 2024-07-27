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

vector<pair<string, int>> readServerConfig(const string &filename);
vector<string> get_all_files_in_directory(const string& directory_path);
string find_file_with_prefix(const vector<string>& file_names, const string& prefix);
vector<joined_row> read_data(const string& filename);
void print_raw_hex(const vector<joined_row>& v);
bool compare_by_row_S(const joined_row& a, const joined_row& b);
void calculate_receiver_and_store(vector<joined_row>& rows, uint32_t n);
vector<tuple<uint32_t, size_t, size_t>> get_first_occurrence_and_count(const vector<joined_row>& sorted_rows);