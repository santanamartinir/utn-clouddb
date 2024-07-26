#include <iostream>
#include <fstream>
#include <vector>
#include <filesystem>
#include <sstream>

namespace fs = std::filesystem;

void splitFile(const std::string &inputFileName, int n_output_files) {
    std::ifstream inputFile(inputFileName);
    if (!inputFile.is_open()) {
        std::cerr << "Error opening input file: " << inputFileName << std::endl;
        return;
    }

    std::vector<std::string> lines;
    std::string line;

    // Read all lines from the input file
    while (std::getline(inputFile, line)) {
        lines.push_back(line);
    }

    inputFile.close();

    int totalLines = lines.size();
    int linesPerFile = totalLines / n_output_files;
    int extraLines = totalLines % n_output_files;

    // Extract the base file name without extension and create the directory
    std::string baseFileName = fs::path(inputFileName).stem().string();
    fs::create_directory(baseFileName);

    int currentLine = 0;
    for (int i = 0; i < n_output_files; ++i) {
        // Construct the output file name
        std::stringstream outputFileName;
        outputFileName << baseFileName << "/" << (i + 1) << "_" << baseFileName << ".txt";

        std::ofstream outputFile(outputFileName.str());
        if (!outputFile.is_open()) {
            std::cerr << "Error opening output file: " << outputFileName.str() << std::endl;
            return;
        }

        int linesToWrite = linesPerFile + (i < extraLines ? 1 : 0);

        for (int j = 0; j < linesToWrite; ++j) {
            if (currentLine < totalLines) {
                outputFile << lines[currentLine++] << "\n";
            }
        }

        outputFile.close();
    }
}

int main(int argc, char *argv[]) {
    if (argc != 3) {
        std::cerr << "Usage: " << argv[0] << " <input_file> <n_output_files>" << std::endl;
        return 1;
    }

    std::string inputFileName = argv[1];
    int n_output_files = std::stoi(argv[2]);

    if (n_output_files <= 0) {
        std::cerr << "Number of output files must be greater than 0" << std::endl;
        return 1;
    }

    splitFile(inputFileName, n_output_files);

    return 0;
}
