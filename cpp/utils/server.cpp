#include <iostream>
#include <boost/asio.hpp>
#include <thread>
#include <string>
#include <utility>
#include <fstream>
#include <vector>
#include "helper_functions.h"

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

int main(int argc, char* argv[]) {
    try {
        if (argc != 5) {
            cerr << "Usage: ./server <server_id> <server_config_file> <R_folder> <S_folder>\n";
            return 1;
        }

        int my_id = atoi(argv[1]);
        string config_file = argv[2];
        string r_folder = argv[3];
        string s_folder = argv[4];

        auto r_files = get_all_files_in_directory(r_folder);
        auto s_files = get_all_files_in_directory(s_folder);

        string r_file = find_file_with_prefix(r_files, to_string(my_id));
        // debug: cout << r_file << endl;
        string s_file = find_file_with_prefix(s_files, to_string(my_id));
        // debug: cout << s_file << endl;

        vector<pair<int, uint64_t>> r_data = read_data(r_folder + '/' + r_file);
        cout << "R len: " << r_data.size() << endl;
        vector<pair<int, uint64_t>> s_data = read_data(s_folder + '/' + s_file);
        cout << "S len: " << s_data.size() << endl;

        vector<pair<string, int>> config = readServerConfig(config_file);
        int n_servers = config.size();

        auto my_ip = config[my_id - 1].first;
        auto my_port = config[my_id - 1].second;

        auto main_server_ip = config[0].first;
        auto main_server_port_str = to_string(config[0].second);

        boost::asio::io_context io_context;

        tcp::resolver resolver(io_context);
        tcp::resolver::results_type endpoints = resolver.resolve(main_server_ip, main_server_port_str);

        tcp::socket socket(io_context);
        boost::asio::connect(socket, endpoints);

        string message = to_string(my_id);
        boost::asio::write(socket, boost::asio::buffer(message));

        char reply[1024];
        boost::system::error_code error;

        size_t length = socket.read_some(boost::asio::buffer(reply), error);
        cout << "Reply is: " << string(reply, length) << endl;



        wait_start_signal(io_context, my_port);


    } catch (exception& e) {
        cerr << "Exception: " << e.what() << "\n";
    }

    return 0;
}
