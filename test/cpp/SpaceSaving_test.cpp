#include "../cpp/utils/SpaceSaving.h"

int main() {
    int k = 3;  // capacity of the histogram
    SpaceSaving ss(k);

    float threshold = 0.32;
    std::vector<int> stream = {1, 1, 1, 1, 1, 2, 3, 4, 5, 6, 7, 8, 85, 5, 4, 3, 2, 3, 3, 6, 3, 2, 2, 1, 1, 1};
    ss.process(stream);

    auto heavy_hitters = ss.get_heavy_hitters(threshold);
    for (const auto& [element, data] : heavy_hitters) {
        std::cout << "Element: " << element << ", Count: " << data.first << ", Frequency: " << data.second << "%" << std::endl;
    }

    return 0;
}
