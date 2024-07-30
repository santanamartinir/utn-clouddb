#include <iostream>
#include <thread>
#include <string>
#include <vector>
#include <zmq.hpp>
#include <algorithm>
#include <tuple>
#include <mutex>
#include <barrier>
#include <unordered_map>
#include <atomic>
#include "./utils/helper_functions.h"
#include "./utils/SpaceSaving.h"

// Function to allocate memory for tuples_data
void allocate_mem(tuples_data& data, size_t size) {
    data.tuples.resize(size);
    data.filled_rows = 0;
}

int copy_local_data_to_s_receive_buffers(
        int my_id,
        const tuples_data& s_data_send,
        std::vector<tuples_data>& s_data_receive,
        const std::unordered_map<int, float>& heavy_hitters) {

    int n_tuples_copied = 0;

    // Copy/send only, if it is not a heavy hitter
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
        // if it is a heavy hitter, it stays on this server
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

    for (const auto& t : r_data_send.tuples) {
        // Check if the tuple is a heavy hitter
        if (heavy_hitters.find(t.join_val) != heavy_hitters.end()) {
            // If it's a heavy hitter, copy to all other servers
            for (int i = 0; i < r_data_receive.size(); ++i) {
                    r_data_receive[i].tuples[r_data_receive[i].filled_rows] = t;
                    r_data_receive[i].filled_rows++;
                // Count only when actually sent
                if (i != my_id) {
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

void node_thread(int id, int n_servers, const std::vector<std::string>& r_files, const std::vector<std::string>& s_files, const std::string& r_folder, const std::string& s_folder,
                 tuples_data& r_data_receive_total, tuples_data& s_data_receive_total, std::mutex& r_mutex, std::mutex& s_mutex, std::barrier<>& sync_point, std::atomic<bool>& done) {
    try {
        zmq::context_t context(1);
        zmq::socket_t receiver(context, zmq::socket_type::pull);
        receiver.bind("tcp://*:" + std::to_string(5550 + id));

        std::unordered_map<int, zmq::socket_t> senders;
        for (int i = 0; i < n_servers; ++i) {
            if (i != id) {
                zmq::socket_t sender(context, zmq::socket_type::push);
                sender.connect("tcp://localhost:" + std::to_string(5550 + i));
                senders[i] = std::move(sender);
            }
        }

        // Read local files
        auto r_data_send_tmp = read_data(r_folder + '/' + r_files[id]);
        auto s_data_send_tmp = read_data(s_folder + '/' + s_files[id]);

        // Prepare data for sending
        tuples_data r_data_send = {r_data_send_tmp, static_cast<int>(r_data_send_tmp.size())};
        tuples_data s_data_send = {s_data_send_tmp, static_cast<int>(s_data_send_tmp.size())};

        // Sample 1% of s_data_send_tmp to estimate heavy hitters
        std::vector<int> sample_stream;
        for (size_t j = 0; j < s_data_send_tmp.size(); j += 100) {
            sample_stream.push_back(s_data_send_tmp[j].join_val);
        }

        // Estimate heavy hitters using SpaceSaving algorithm
        SpaceSaving::DataStructure ds = SpaceSaving::HashTableOnly;
        int k = 128;  // Capacity of the histogram for heavy hitter detection
        SpaceSaving ss(k, ds);
        
        ss.process(sample_stream);

        float threshold = 0.01;  // Define a threshold
        auto heavy_hitters = ss.get_heavy_hitters(threshold);

        // Print detected heavy hitters
        std::cout << "Heavy Hitters:" << std::endl;
        for (const auto& [element, frequency] : heavy_hitters) {
            std::cout << "Element: " << element << ", Frequency: " << frequency * 100 << "%" << std::endl;
        }

        // Process local data
        calculate_receiver_and_store(s_data_send.tuples, n_servers);
        calculate_receiver_and_store(r_data_send.tuples, n_servers);

        // Synchronize before sending data
        sync_point.arrive_and_wait();

        // Send S data to other nodes
        int num_s_tuples_sent = 0;
        for (const auto& t : s_data_send.tuples) {
            if (heavy_hitters.find(t.join_val) == heavy_hitters.end()) {
                int target_server = t.row_S - 1;
                if (target_server != id) {
                    zmq::message_t message(sizeof(joined_row) + sizeof(char));
                    char header = 'S';
                    memcpy(message.data(), &header, sizeof(char));
                    memcpy(static_cast<char*>(message.data()) + sizeof(char), &t, sizeof(joined_row));
                    if (senders.find(target_server) != senders.end()) {
                        senders[target_server].send(message, zmq::send_flags::none);
                        num_s_tuples_sent++;
                    } else {
                        std::cerr << "Invalid target server: " << target_server << " in node " << id << std::endl;
                    }
                }
            } else {
                // Heavy hitter stays on this server
                std::lock_guard<std::mutex> lock(s_mutex);
                s_data_receive_total.tuples[s_data_receive_total.filled_rows++] = t;
            }
        }

        // Send R data to other nodes
        int num_r_tuples_sent = 0;
        for (const auto& t : r_data_send.tuples) {
            if (heavy_hitters.find(t.join_val) != heavy_hitters.end()) {
                for (int i = 0; i < n_servers; ++i) {
                    if (i != id) {
                        zmq::message_t message(sizeof(joined_row) + sizeof(char));
                        char header = 'R';
                        memcpy(message.data(), &header, sizeof(char));
                        memcpy(static_cast<char*>(message.data()) + sizeof(char), &t, sizeof(joined_row));
                        senders[i].send(message, zmq::send_flags::none);
                        num_r_tuples_sent++;
                    }
                }
            } else {
                int target_server = t.row_S - 1;
                if (target_server != id) {
                    zmq::message_t message(sizeof(joined_row) + sizeof(char));
                    char header = 'R';
                    memcpy(message.data(), &header, sizeof(char));
                    memcpy(static_cast<char*>(message.data()) + sizeof(char), &t, sizeof(joined_row));
                    if (senders.find(target_server) != senders.end()) {
                        senders[target_server].send(message, zmq::send_flags::none);
                        num_r_tuples_sent++;
                    } else {
                        std::cerr << "Invalid target server: " << target_server << " in node " << id << std::endl;
                    }
                }
            }
        }

        std::cout << "Node " << id << " sent " << num_s_tuples_sent << " S tuples and " << num_r_tuples_sent << " R tuples." << std::endl;

        // Thread to handle receiving messages
        std::thread receive_thread([&receiver, &s_data_receive_total, &r_data_receive_total, &r_mutex, &s_mutex, id, &done]() {
            try {
                while (!done.load()) {
                    zmq::message_t message;
                    auto result = receiver.recv(message, zmq::recv_flags::dontwait);
                    if (result) {
                        char header = *static_cast<char*>(message.data());
                        auto received_data = reinterpret_cast<joined_row*>(static_cast<char*>(message.data()) + sizeof(char));
                        if (header == 'R') {
                            std::lock_guard<std::mutex> lock(r_mutex);
                            r_data_receive_total.tuples[r_data_receive_total.filled_rows++] = *received_data;
                        } else if (header == 'S') {
                            std::lock_guard<std::mutex> lock(s_mutex);
                            s_data_receive_total.tuples[s_data_receive_total.filled_rows++] = *received_data;
                        } else {
                            std::cerr << "Invalid message header: " << header << " in node " << id << std::endl;
                        }

                        std::cout << "Node " << id << " received a tuple." << std::endl;
                    } else {
                        std::this_thread::sleep_for(std::chrono::milliseconds(100));  // Sleep to avoid busy-waiting
                    }
                }
            } catch (const zmq::error_t& e) {
                std::cerr << "Receive thread error in node " << id << ": " << e.what() << std::endl;
            }
        });

        receive_thread.join();
    } catch (const zmq::error_t& e) {
        std::cerr << "Error in node " << id << ": " << e.what() << std::endl;
    }
}

int main(int argc, char* argv[]) {
    try {
        if (argc != 6) {
            std::cerr << "Usage: ./server <n_servers> <num_r_tuples> <num_s_tuples> <R_folder> <S_folder>\n";
            return 1;
        }

        int n_servers = std::stoi(argv[1]);
        int num_r_tuples = std::stoi(argv[2]);
        int num_s_tuples = std::stoi(argv[3]);
        std::string r_folder = argv[4];
        std::string s_folder = argv[5];

        auto r_files = get_all_files_in_directory(r_folder);
        auto s_files = get_all_files_in_directory(s_folder);

        // Ensure there are enough files for the number of servers
        if (r_files.size() < n_servers || s_files.size() < n_servers) {
            std::cerr << "Not enough R or S files for the number of servers.\n";
            return 1;
        }

        // Allocate memory for total receive buffers
        tuples_data r_data_receive_total, s_data_receive_total;
        allocate_mem(r_data_receive_total, num_r_tuples * n_servers);
        allocate_mem(s_data_receive_total, num_s_tuples * n_servers);

        // Mutexes for synchronizing access to the receive buffers
        std::mutex r_mutex, s_mutex;

        // Atomic flag to signal threads to stop
        std::atomic<bool> done(false);

        // Synchronization barrier to ensure all nodes are ready before proceeding
        std::barrier sync_point(n_servers);

        // Start a thread for each node
        std::vector<std::thread> nodes;
        for (int i = 0; i < n_servers; ++i) {
            nodes.emplace_back(node_thread, i, n_servers, r_files, s_files, r_folder, s_folder,
                               std::ref(r_data_receive_total), std::ref(s_data_receive_total), std::ref(r_mutex), std::ref(s_mutex), std::ref(sync_point), std::ref(done));
        }

        for (auto& node : nodes) {
            node.join();
        }

        // Signal threads to stop
        done.store(true);

        // Wait for a moment to ensure all threads have stopped
        std::this_thread::sleep_for(std::chrono::seconds(1));

        // Perform the join operation on the received data
        // Create a hash table for R
        std::unordered_multimap<int, joined_row> r_hash_table;
        {
            std::lock_guard<std::mutex> lock(r_mutex);
            for (const auto& row : r_data_receive_total.tuples) {
                r_hash_table.insert({row.join_val, row});
            }
        }

        // Iterate through S and join with R
        std::vector<joined_row> join_result;
        {
            std::lock_guard<std::mutex> lock(s_mutex);
            for (const auto& row : s_data_receive_total.tuples) {
                auto range = r_hash_table.equal_range(row.join_val);
                for (auto it = range.first; it != range.second; ++it) {
                    joined_row joined;
                    joined.join_val = row.join_val;
                    joined.row_R = it->second.row_R;
                    joined.row_S = row.row_S;
                    join_result.push_back(joined);
                }
            }
        }

        // Output the join result
        for (const auto& row : join_result) {
            std::cout << "Joined Row: Key=" << row.join_val << ", Value_R=" << row.row_R << ", Value_S=" << row.row_S << std::endl;
        }

    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << "\n";
    }

    return 0;
}
