#include <iostream>
#include <boost/asio.hpp>
#include <thread>
#include <string>

using boost::asio::ip::tcp;
using namespace::std;

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

void send(){

}

int main(int argc, char* argv[]) {
    try {
        if (argc != 2) {
            cerr << "Usage: ./server_send_receive_test <port>\n";
            return 1;
        }

        boost::asio::io_context io_context;
        //server(io_context, atoi(argv[1]));
        
        thread t1(server, ref(io_context), atoi(argv[1]));
        for(;;){
            std::string ip;
            std::string port;
            std::string msg;
            
            cout << "To send data, enter ip of receiver: ";
            cin >> ip;

            cout << "and its port: ";
            cin >> port;

            boost::asio::io_context io_context;

            tcp::resolver resolver(io_context);
            tcp::resolver::results_type endpoints = resolver.resolve(ip, port);

            tcp::socket socket(io_context);
            boost::asio::connect(socket, endpoints);

            std::string message = "Hello from " + string(port);
            boost::asio::write(socket, boost::asio::buffer(message));

            char reply[1024];
            size_t reply_length = boost::asio::read(socket, boost::asio::buffer(reply, message.size()));
            std::cout << "Reply is: ";
            std::cout.write(reply, reply_length);
            std::cout << "\n";
        }

    } catch (exception& e) {
        cerr << "Exception: " << e.what() << "\n";
    }

    return 0;
}
