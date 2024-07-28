#include <iostream>
#include <boost/asio.hpp>
#include <thread>
#include <string>
#include <utility>
#include <fstream>
#include <vector>
#include <filesystem>
#include "helper_functions.h"
#include "SpaceSaving.h"
#include <algorithm>

struct tuples_data {
    std::vector<joined_row> tuples;
    int filled_rows;
};

int copy_local_data_to_receive_buffers(
        int my_id,
        const tuples_data& s_data_send,
        std::vector<tuples_data>& s_data_receive,
        const std::vector<std::tuple<uint32_t,
        size_t, size_t>>& memory_locations,
        std::vector<std::pair<int, std::pair<int, float>>> heavy_hitters) {

    int i = 0;
    int n_tuples_copied = 0;

    for (const auto& [server_id, offset, count] : memory_locations) {
        bool is_not_heavy_hitter = true;
        if (offset + count > s_data_send.tuples.size()) {
            throw std::out_of_range("Offset and count exceed the size of s_data_send");
        }

        for(auto h : heavy_hitters){
            if(s_data_send.tuples[offset].join_val == h.first)
            {
                is_not_heavy_hitter = false;
            }
        }

        if( is_not_heavy_hitter ){
            // Copy the data from s_data_send to the beginning of s_data_receive
            std::copy(s_data_send.tuples.begin() + offset, s_data_send.tuples.begin() + offset + count, s_data_receive[i].tuples.begin() + s_data_receive[i].filled_rows);
            s_data_receive[i].filled_rows += count;
            if(my_id != i) { // own files do not need to be sent over the network
                n_tuples_copied += count;
            }
        }
        i++;
    }

    return n_tuples_copied;
}

// Function to allocate memory for a vector of vectors
void allocate_mem_dual_vec(std::vector<tuples_data>& vec, size_t outer_size, size_t inner_size) {
    // Resize the outer vector to the required size
    vec.resize(outer_size);

    // Resize each inner vector to the required size
    for (size_t i = 0; i < outer_size; ++i) {
        vec[i].tuples.resize(inner_size);
        vec[i].filled_rows = 0;
    }
}

int main(int argc, char* argv[]) {
    try {
        if (argc != 6) {
            std::cerr << "Usage: ./server <n_servers> <num_r_tuples> <num_s_tuples> <R_folder> <S_folder>\n";
            return 1;
        }

        // Get Arguments
        int n_servers = std::atoi(argv[1]);
        int num_r_tuples = std::atoi(argv[2]);
        int num_s_tuples = std::atoi(argv[3]);
        std::string r_folder = argv[4];
        std::string s_folder = argv[5];

        std::vector<tuples_data> r_data_send;
        allocate_mem_dual_vec(r_data_send, n_servers, num_r_tuples);
        std::vector<tuples_data> s_data_send;
        allocate_mem_dual_vec(s_data_send, n_servers, num_s_tuples);
        std::vector<tuples_data> r_data_receive;
        allocate_mem_dual_vec(r_data_receive, n_servers, num_r_tuples);
        std::vector<tuples_data> s_data_receive;
        allocate_mem_dual_vec(s_data_receive, n_servers, num_s_tuples);

        uint32_t num_tuples_sent = 0;
        std::vector<int> sample_stream;

        for(int i = 0; i < n_servers; i++ ) {
            // Get and find files
            auto r_files = get_all_files_in_directory(r_folder);
            auto s_files = get_all_files_in_directory(s_folder);

            std::string r_file = find_file_with_prefix(r_files, std::to_string(i + 1));
            std::string s_file = find_file_with_prefix(s_files, std::to_string(i + 1));

            // Read local data to this server
            auto r_data_send_tmp = read_data(r_folder + '/' + r_file);
            auto s_data_send_tmp = read_data(s_folder + '/' + s_file);

            r_data_send[i].tuples = r_data_send_tmp;
            r_data_send[i].filled_rows = r_data_send_tmp.size();
            s_data_send[i].tuples = s_data_send_tmp;
            s_data_send[i].filled_rows = s_data_send_tmp.size();

            // Sample 1% of r_data_send_tmp to estimate heavy hitters
            for (size_t j = 0; j < s_data_send_tmp.size(); j += 1) {
                sample_stream.push_back(s_data_send_tmp[j].join_val);
            }
        }

        // Estimate heavy hitters using SpaceSaving algorithm
        int k = 128;  // capacity of the histogram for heavy hitter detection
        SpaceSaving ss(k);
        ss.process(sample_stream);

        float threshold = 0.01; // Define a threshold
        auto heavy_hitters = ss.get_heavy_hitters(threshold);

        std::cout << "Heavy Hitters:" << std::endl;
        for (const auto& [element, data] : heavy_hitters) {
            std::cout << "Element: " << element << ", Count: " << data.first << ", Frequency: " << data.second * 100 << "%" << std::endl;
        }

        for(int i = 0; i < n_servers; i++ ) {
            // Process local data
            calculate_receiver_and_store(s_data_send[i].tuples, n_servers); // Stores server id in third col
            std::sort(s_data_send[i].tuples.begin(), s_data_send[i].tuples.end(), compare_by_row_S); // Sort by third col
            auto memory_locations = get_first_occurrence_and_count(s_data_send[i].tuples); // Get memory locations and lengths of specific server data
            num_tuples_sent += copy_local_data_to_receive_buffers(i, s_data_send[i], s_data_receive, memory_locations, heavy_hitters);    
        }
    
    } catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << "\n";
    }

    return 0;
}
