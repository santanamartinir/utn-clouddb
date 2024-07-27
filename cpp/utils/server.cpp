#include <iostream>
#include <boost/asio.hpp>
#include <thread>
#include <string>
#include <utility>
#include <fstream>
#include <vector>
#include "helper_functions.h"
#include <chrono>

using boost::asio::ip::tcp;

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

void wait_start_signal(boost::asio::io_context& io_context, short my_port) {

    tcp::acceptor acceptor(io_context, tcp::endpoint(tcp::v4(), my_port));

    tcp::socket socket(io_context);
    acceptor.accept(socket);
    char data[1024] = {0}; // Clear the buffer
    boost::system::error_code error;

    size_t length = socket.read_some(boost::asio::buffer(data), error);
    if (error == boost::asio::error::eof)
        return;
    else if (error)
        throw boost::system::system_error(error); // Some other error.

    cout << "Msg: " << string(data, length) << endl;

    string response = "Starting";

    // Echo back the message to main server
    boost::asio::write(socket, boost::asio::buffer(response, response.length()));
}

// Function to notify the main server
void notify_main_server(boost::asio::io_context& io_context, const string& main_server_ip, const string& main_server_port_str, int my_id) {
    try {
        // Create a resolver and query
        tcp::resolver resolver(io_context);
        tcp::resolver::results_type endpoints = resolver.resolve(main_server_ip, main_server_port_str);

        // Create and connect the socket
        tcp::socket socket(io_context);
        boost::asio::connect(socket, endpoints);

        // Prepare and send the message
        string message = to_string(my_id);
        boost::asio::write(socket, boost::asio::buffer(message));

        // Prepare to receive the reply
        char reply[1024];
        boost::system::error_code error;

        // Read the reply from the main server
        size_t length = socket.read_some(boost::asio::buffer(reply), error);
        if (error) {
            throw boost::system::system_error(error); // Handle error
        }

        // Print the reply
        cout << "Reply is: " << string(reply, length) << endl;

    } catch (const exception& e) {
        cerr << "Exception in notify_main_server: " << e.what() << endl;
    }
}

int main(int argc, char* argv[]) {
    try {
        if (argc != 5) {
            cerr << "Usage: ./server <server_id> <server_config_file> <R_folder> <S_folder>\n";
            return 1;
        }

        // Get Arguments
        int my_id = atoi(argv[1]);
        string config_file = argv[2];
        string r_folder = argv[3];
        string s_folder = argv[4];

        // Get and find files
        auto r_files = get_all_files_in_directory(r_folder);
        auto s_files = get_all_files_in_directory(s_folder);

        string r_file = find_file_with_prefix(r_files, to_string(my_id));
        string s_file = find_file_with_prefix(s_files, to_string(my_id));

        // Read local data to this server
        vector<joined_row> r_data_send = read_data(r_folder + '/' + r_file);
        vector<joined_row> s_data_send = read_data(s_folder + '/' + s_file);

        // Read server configuration file
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

        auto main_server_ip = config[0].first;
        auto main_server_port_str = to_string(config[0].second);

        boost::asio::io_context io_context;
        notify_main_server(io_context, main_server_ip, main_server_port_str, my_id);

        wait_start_signal(io_context, my_port);

            // deb: print_raw_hex(s_data_send);
        calculate_receiver_and_store(s_data_send, n_servers);

        // Sort the vector by row_S
        sort(s_data_send.begin(), s_data_send.end(), compare_by_row_S);
        
        notify_main_server(io_context, main_server_ip, main_server_port_str, my_id);

        wait_start_signal(io_context, my_port);



        auto  memory_locations = get_first_occurrence_and_count(s_data_send);

#if 0
        send_data_to_all_servers(my_id, config, s_data_send, memory_locations);
        thread receiving_thread(receive_data_from_all_servers,
                                ref(io_context),
                                my_port,
                                ref(s_data_receive),
                                my_id, n_servers);
#endif

    } catch (exception& e) {
        cerr << "Exception: " << e.what() << "\n";
    }

    return 0;
}
