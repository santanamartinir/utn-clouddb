#include <iostream>
#include <fstream>
#include <string>
#include <cstdlib> // For std::atoi

int main(int argc, char* argv[]) {
    // Check if the correct number of arguments is provided
    if (argc != 2) {
        std::cerr << "Usage: " << argv[0] << " <N>" << std::endl;
        return 1;
    }

    // Parse the command line arguments
    int N = std::atoi(argv[1]);

    // Construct the output file name
    std::string filename = std::to_string(N) + ".txt";

    // Open the output file
    std::ofstream outputFile(filename);
    if (!outputFile) {
        std::cerr << "Error opening output file: " << filename << std::endl;
        return 1;
    }

    // Write numbers 1 to N to the file
    for (int i = 1; i <= N; ++i) {
        outputFile << i << std::endl;
    }

    outputFile.close();
    std::cout << "Numbers 1 to " << N << " have been written to " << filename << std::endl;
    return 0;
}