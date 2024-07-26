#include <iostream>
#include <fstream>
#include <vector>
#include <random>
#include <string>

using namespace std;

// Function to read integers from a file
std::vector<int> readIntegersFromFile(const std::string& filename) {
    std::vector<int> integers;
    std::ifstream inputFile(filename);
    if (!inputFile) {
        std::cerr << "Error opening input file: " << filename << std::endl;
        return integers;
    }
    int value;
    while (inputFile >> value) {
        integers.push_back(value);
    }
    inputFile.close();
    return integers;
}

// Function to write integer-row number pairs to a file
void writePairsToFile(const std::string& filename, const std::vector<int>& integers) {
    std::ofstream outputFile(filename);
    if (!outputFile) {
        std::cerr << "Error opening output file: " << filename << std::endl;
        return;
    }
    for (uint64_t i = 0; i < integers.size(); i++) {
        outputFile << integers[i] << " " << i + 1 << std::endl;
    }
    outputFile.close();
}

int main(int argc, char* argv[]) {

    // Check if the correct number of arguments are provided
    if (argc != 3) {
        cerr << "Error: Incorrect number of arguments.\n";
        cerr << "Usage: " << argv[0] << " <input file> <prefix>\n";
        return 1;
    }

    // Parse command-line arguments
    string inputFilename  = argv[1];
    string prefix  = argv[2];
    string outputFilename = prefix + "_" + inputFilename;

    // Step 1: Read integers from the input file
    std::vector<int> integers = readIntegersFromFile(inputFilename);
    if (integers.empty()) {
        std::cerr << "No integers read from file or file is empty." << std::endl;
        return 1;
    }

    // Step 2: Write integer-row number pairs to the output file
    writePairsToFile(outputFilename, integers);

    std::cout << "Integer-row number pairs written to " << outputFilename << std::endl;
    return 0;
}
