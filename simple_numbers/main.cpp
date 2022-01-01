#include <cmath>
#include <iostream>
#include <vector>

constexpr int32_t TARGET = 10'000'000;

int main() {
    std::vector<int32_t> vec;
    for (int32_t i = 2; i <= TARGET; ++i) {
        bool flag = false;
        for (int32_t prime : vec) {
            if (i % prime == 0) {
                flag = true;
                break;
            }
            if (prime > sqrt(i)) {
                break;
            }
        }
        if (!flag) {
            vec.push_back(i);
        }
    }
    std::cout << vec.back() << std::endl;
}