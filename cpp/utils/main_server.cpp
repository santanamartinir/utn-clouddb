#include <iostream>
#include <boost/asio.hpp>
#include <thread>
#include <string>
#include <utility>
#include <fstream>
#include <vector>
#include <filesystem>
#include "helper_functions.h"
#include <algorithm>

using boost::asio::ip::tcp;

void wait_for_reads(boost::asio::io_context& io_context, short port, int n_servers) {
    cout << "Waiting for all servers to finish reading" << endl;
    tcp::acceptor acceptor(io_context, tcp::endpoint(tcp::v4(), port));

    while ((n_servers - 1) > 0) {
        tcp::socket socket(io_context);
        acceptor.accept(socket);
        char data[1024] = {0}; // Clear the buffer
        boost::system::error_code error;

        size_t length = socket.read_some(boost::asio::buffer(data), error);
        if (error == boost::asio::error::eof)
            break; // Connection closed cleanly by peer.
        else if (error)
            throw boost::system::system_error(error); // Some other error.

        cout << "Server: " << string(data, length) << " finished reading" << endl; // Use length to correctly display received data

        string response = "Received acknowledgment";

        // Echo back the message to the client
        boost::asio::write(socket, boost::asio::buffer(response, response.length()));

        // Decrement the server count
        n_servers--;
    }
}

void session(tcp::socket socket) {
    try {
        for (;;) {
            char data[1024];
            boost::system::error_code error;
            cout << "in session loop" << endl;

            size_t length = socket.read_some(boost::asio::buffer(data), error);
            if (error == boost::asio::error::eof)
                break; // Connection closed cleanly by peer.
            else if (error)
                throw boost::system::system_error(error); // Some other error.

            cout << "Received: " << string(data, length) << endl;

            // Echo back the message to the client
            boost::asio::write(socket, boost::asio::buffer(data, length));
        }
    } catch (exception& e) {
        cerr << "Exception in thread: " << e.what() << "\n";
    }
}

void server(boost::asio::io_context& io_context, short port) {
    tcp::acceptor acceptor(io_context, tcp::endpoint(tcp::v4(), port));
    for (;;) {
        tcp::socket socket(io_context);
        acceptor.accept(socket);
        thread(session, move(socket)).detach();
    }
}



void start_other_servers(boost::asio::io_context& io_context, std::vector<std::pair<std::string, int>> config){
    auto n_servers = config.size() - 1;

    for(int i = 1; i <= n_servers; i++){
        tcp::resolver resolver(io_context);
        tcp::resolver::results_type endpoints = resolver.resolve(config[i].first, to_string(config[i].second));

        tcp::socket socket(io_context);
        boost::asio::connect(socket, endpoints);

        string message = "Start";
        boost::asio::write(socket, boost::asio::buffer(message));

        char reply[1024];
        boost::system::error_code error;

        size_t length = socket.read_some(boost::asio::buffer(reply), error);
        cout << "Reply is: " << string(reply, length) << endl;
    }
}

void send_tuple_to_server(joined_row* pRow, string server_ip, string server_port){
        boost::asio::io_context io_context;

        tcp::resolver resolver(io_context);
        tcp::resolver::results_type endpoints = resolver.resolve(server_ip, server_port);

        tcp::socket socket(io_context);
        boost::asio::connect(socket, endpoints);

        boost::asio::write(socket, boost::asio::buffer(pRow, sizeof(joined_row)));
}

int main(int argc, char* argv[]) {
    try {
        if (argc != 5) {
            cerr << "Usage: ./server <server_id> <server_config_file> <R_folder> <S_folder>\n";
            return 1;
        }

        // Get Arguments
        int my_id = 1;
        string config_file = argv[2];
        string r_folder = argv[3];
        string s_folder = argv[4];

        // Get and find files
        auto r_files = get_all_files_in_directory(r_folder);
        auto s_files = get_all_files_in_directory(s_folder);

        string r_file = find_file_with_prefix(r_files, to_string(my_id));
        string s_file = find_file_with_prefix(s_files, to_string(my_id));

        // Read local data to this server
        auto r_data_send = read_data(r_folder + '/' + r_file);
        auto s_data_send = read_data(s_folder + '/' + s_file);

        vector<pair<string, int>> config = readServerConfig(config_file);
        int n_servers = config.size();
        auto my_ip = config[my_id - 1].first;
        auto my_port = config[my_id - 1].second;

        // Calculate total number of tuples
        auto num_total_r_tuples = n_servers * r_data_send.size();
        auto num_total_s_tuples = n_servers * s_data_send.size();

        // Allocate memory to receive data
        vector<joined_row> r_data_receive(num_total_r_tuples);
        vector<joined_row> s_data_receive(num_total_s_tuples);

        // Wait until all other servers have finished reading
        boost::asio::io_context io_context;
        wait_for_reads(io_context, my_port, n_servers);

        // Process local data
        calculate_receiver_and_store(s_data_send, n_servers); // Stores server id in third col
        std::sort(s_data_send.begin(), s_data_send.end(), compare_by_row_S); // Sort by third col
        // Get memory locations and lengths of specific server data
        auto  memory_locations = get_first_occurrence_and_count(s_data_send);
        // ToDo: Copy local data to first part of receive data

        // Start processing on other servers
        start_other_servers(io_context, config);

        // wait for all servers to finish processing
        wait_for_reads(io_context, my_port, n_servers);

        start_other_servers(io_context, config);

        //send_vec_to_server(s_data_send, config[1].first, to_string(config[1].second),0,s_data_send.size());
        //send_data_to_all_servers(my_id, config, s_data_send, memory_locations);
    } catch (exception& e) {
        cerr << "Exception: " << e.what() << "\n";
    }

    return 0;
}