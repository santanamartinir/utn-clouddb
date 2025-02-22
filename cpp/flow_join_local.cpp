#include <iostream>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <chrono>
#include "./utils/helper_functions.h"
#include "./utils/SpaceSaving.h"

int copy_local_data_to_s_receive_buffers(
        int my_id,
        const tuples_data& s_data_send,
        std::vector<tuples_data>& s_data_receive,
        const std::unordered_map<int, float>& heavy_hitters) {

    int n_tuples_copied = 0;

    // Copy/send only, if it is not a heavy hitter
    // Iterate over tuples to process them
    for(const auto& t : s_data_send.tuples) {
        // Check if the tuple is not a heavy hitter
        if (heavy_hitters.find(t.join_val) == heavy_hitters.end()) {
            // Copy to the receive buffer corresponding to the target row
            s_data_receive[t.row_S - 1].tuples[s_data_receive[t.row_S - 1].filled_rows] = t;
            s_data_receive[t.row_S - 1].filled_rows++;
            if((t.row_S - 1) != my_id){ // Only count if not sent/copied to other servers
                n_tuples_copied++;
            }
        }
        // If it is a heavy hitter, it stays on this server
        else {
            s_data_receive[my_id].tuples[s_data_receive[my_id].filled_rows] = t;
            s_data_receive[my_id].filled_rows++;
        }
    }

    return n_tuples_copied;
}

int copy_local_data_to_r_receive_buffers(
        int my_id,
        const tuples_data& r_data_send,
        std::vector<tuples_data>& r_data_receive,
        const std::unordered_map<int, float>& heavy_hitters) {

    int n_tuples_copied = 0;

    // Iterate over tuples to process them
    for (const auto& t : r_data_send.tuples) {
        // Check if the tuple is a heavy hitter
        if (heavy_hitters.find(t.join_val) != heavy_hitters.end()) {
            // If it's a heavy hitter, copy to all other servers
            for (int i = 0; i < r_data_receive.size(); ++i) {
                    r_data_receive[i].tuples[r_data_receive[i].filled_rows] = t;
                    r_data_receive[i].filled_rows++;
                if (i != my_id) { // Increment only when actually sent
                    n_tuples_copied++;
                }
            }
        // Otherwise, copy to the corresponding server
        } else {
            r_data_receive[t.row_S - 1].tuples[r_data_receive[t.row_S - 1].filled_rows] = t;
            r_data_receive[t.row_S - 1].filled_rows++;
            n_tuples_copied++;
        }
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
        // Check if the number of arguments is correct
        if (argc != 6) {
            std::cerr << "Usage: ./flow_join_local <n_servers> <num_r_tuples> <num_s_tuples> <R_folder> <S_folder>\n";
            return 1;
        }

        // Get arguments
        int n_servers = std::atoi(argv[1]);
        int num_r_tuples = std::atoi(argv[2]);
        int num_s_tuples = std::atoi(argv[3]);
        std::string r_folder = argv[4];
        std::string s_folder = argv[5];

        // Initialize vectors
        std::vector<tuples_data> r_data_send;
        allocate_mem_dual_vec(r_data_send, n_servers, num_r_tuples);
        std::vector<tuples_data> s_data_send;
        allocate_mem_dual_vec(s_data_send, n_servers, num_s_tuples);
        std::vector<tuples_data> r_data_receive;
        allocate_mem_dual_vec(r_data_receive, n_servers, num_r_tuples);
        std::vector<tuples_data> s_data_receive;
        allocate_mem_dual_vec(s_data_receive, n_servers, num_s_tuples);

        uint32_t num_s_tuples_sent = 0;
        uint32_t num_r_tuples_sent = 0;

        std::vector<int> sample_stream;

        // Loop over each server to process local data
        for(int i = 0; i < n_servers; i++ ) {
            // Get and find files
            auto r_files = get_all_files_in_directory(r_folder);
            auto s_files = get_all_files_in_directory(s_folder);
            
            // Find files for this server
            std::string r_file = find_file_with_prefix(r_files, std::to_string(i + 1));
            std::string s_file = find_file_with_prefix(s_files, std::to_string(i + 1));

            // Read local data to this server
            auto r_data_send_tmp = read_data(r_folder + '/' + r_file);
            auto s_data_send_tmp = read_data(s_folder + '/' + s_file);
            
            // Store data in vectors
            r_data_send[i].tuples = r_data_send_tmp;
            r_data_send[i].filled_rows = r_data_send_tmp.size();
            s_data_send[i].tuples = s_data_send_tmp;
            s_data_send[i].filled_rows = s_data_send_tmp.size();

            // Sample 1% of r_data_send_tmp to estimate heavy hitters
            for (size_t j = 0; j < s_data_send_tmp.size(); j += 100) { // Previously j += 1
                sample_stream.push_back(s_data_send_tmp[j].join_val);
            }
        }

        auto start = std::chrono::high_resolution_clock::now(); // Start time
        // Estimate heavy hitters using SpaceSaving algorithm
        SpaceSaving::DataStructure ds = SpaceSaving::SortedArray; // Define here data structure to be use
        int k = 128;  // Capacity of the histogram for heavy hitter detection
        SpaceSaving ss(k, ds);
        
        ss.process(sample_stream);

        float threshold = 0.01;  // Define a threshold
        auto heavy_hitters = ss.get_heavy_hitters(threshold);

        auto end_time = std::chrono::high_resolution_clock::now(); // End time

        // Calculate and print execution time
        std::chrono::duration<double> elapsed = end_time - start;
        std::cout << "Heavy hitter detection took " << elapsed.count() << " seconds.\n";

        // Print detected heavy hitters
        std::cout << "Heavy Hitters:" << std::endl;
        for (const auto& [element, frequency] : heavy_hitters) {
            std::cout << "Element: " << element << ", Frequency: " << frequency * 100 << "%" << std::endl;
        }

        // Process local data
        for(int i = 0; i < n_servers; i++ ) {
            calculate_receiver_and_store(s_data_send[i].tuples, n_servers); // Stores server id in third col
            calculate_receiver_and_store(r_data_send[i].tuples, n_servers); // Stores server id in third col
            num_s_tuples_sent += copy_local_data_to_s_receive_buffers(i, s_data_send[i], s_data_receive, heavy_hitters);
            num_r_tuples_sent += copy_local_data_to_r_receive_buffers(i, r_data_send[i], r_data_receive, heavy_hitters); 
        }

        // Open a file to save execution times
        std::ofstream output_file("execution_times.txt");
        if (!output_file.is_open()) {
            std::cerr << "Failed to open file for writing execution times.\n";
            return 1;
        }
        for(int i = 0; i < n_servers; i++){
            auto start = std::chrono::high_resolution_clock::now(); // Start time
            auto r_join_s = inner_join(r_data_receive[i], s_data_receive[i]);
            auto end_time = std::chrono::high_resolution_clock::now(); // End time

            // Calculate and print execution time
            std::chrono::duration<double> elapsed = end_time - start;
            std::cout << "Server " << i << " inner join took " << elapsed.count() << " seconds.\n";

            // Save the execution time to the file
            output_file << "Server " << i << ": " << elapsed.count() << " seconds\n";
        }       
    } catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << "\n";
    }

    return 0;
}