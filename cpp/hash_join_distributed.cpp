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
#include "./utils/helper_functions.h"

// Function to allocate memory for tuples_data
void allocate_mem(tuples_data& data, size_t size) {
    data.tuples.resize(size);
    data.filled_rows = 0;
}

void node_thread(int id, int n_servers, const std::vector<std::string>& r_files, const std::vector<std::string>& s_files, const std::string& r_folder, const std::string& s_folder,
                 tuples_data& r_data_receive_total, tuples_data& s_data_receive_total, std::mutex& r_mutex, std::mutex& s_mutex, std::barrier<>& sync_point) {
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

        // Process local data
        calculate_receiver_and_store(s_data_send.tuples, n_servers);  // Assumes this function modifies the third column to store server ids
        std::sort(s_data_send.tuples.begin(), s_data_send.tuples.end(), compare_by_row_S);
        auto memory_locations = get_first_occurrence_and_count(s_data_send.tuples);

        // Synchronize before sending data
        sync_point.arrive_and_wait();

        // Send data to other nodes
        for (const auto& [server_id, offset, count] : memory_locations) {
            int sender_index = server_id - 1;
            if (sender_index != id) {
                std::vector<joined_row> data_to_send(s_data_send.tuples.begin() + offset, s_data_send.tuples.begin() + offset + count);
                zmq::message_t message(data_to_send.size() * sizeof(joined_row));
                memcpy(message.data(), data_to_send.data(), data_to_send.size() * sizeof(joined_row));
                if (senders.find(sender_index) != senders.end()) {
                    std::cout << "Node " << id << " sending data to node " << sender_index << std::endl;
                    senders[sender_index].send(message, zmq::send_flags::none);
                } else {
                    std::cerr << "Invalid sender index: " << sender_index << " in node " << id << std::endl;
                }
            }
        }

        // Thread to handle receiving messages
        std::thread receive_thread([&receiver, &s_data_receive_total, &s_mutex, id]() {
            try {
                while (true) {
                    zmq::message_t message;
                    auto result = receiver.recv(message, zmq::recv_flags::none);
                    if (result) {
                        auto received_data = static_cast<joined_row*>(message.data());
                        size_t count = message.size() / sizeof(joined_row);

                        // Lock the mutex before modifying the receive buffer
                        std::lock_guard<std::mutex> lock(s_mutex);
                        s_data_receive_total.tuples.insert(s_data_receive_total.tuples.end(), received_data, received_data + count);
                        s_data_receive_total.filled_rows += count;

                        std::cout << "Node " << id << " received " << count << " tuples." << std::endl;
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

        // Synchronization barrier to ensure all nodes are ready before proceeding
        std::barrier sync_point(n_servers);

        // Start a thread for each node
        std::vector<std::thread> nodes;
        for (int i = 0; i < n_servers; ++i) {
            nodes.emplace_back(node_thread, i, n_servers, r_files, s_files, r_folder, s_folder,
                               std::ref(r_data_receive_total), std::ref(s_data_receive_total), std::ref(r_mutex), std::ref(s_mutex), std::ref(sync_point));
        }

        for (auto& node : nodes) {
            node.join();
        }

        // Perform the join operation on the received data
        // TODO: Implement the join logic using r_data_receive_total and s_data_receive_total

    } catch (const std::exception& e) {
        std::cerr << "Exception: " << e.what() << "\n";
    }

    return 0;
}
