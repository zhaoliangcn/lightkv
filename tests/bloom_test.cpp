#include "lightkv/bloom_filter.h"
#include <iostream>
#include <cassert>
#include <string>

void TestBloomFilter() {
    lightkv::BloomFilter filter(10);

    filter.Add("hello");
    filter.Add("world");
    filter.Add("lightkv");

    assert(filter.MayMatch("hello"));
    assert(filter.MayMatch("world"));
    assert(filter.MayMatch("lightkv"));

    // May have false positives, but these should very likely be false
    // We can't assert !MayMatch, but we can test many keys
    int false_positives = 0;
    for (int i = 0; i < 10000; ++i) {
        if (filter.MayMatch("nonexistent_" + std::to_string(i))) {
            ++false_positives;
        }
    }
    std::cout << "False positives out of 10000: " << false_positives << std::endl;
    // With 3 inserts and 10 bits/key, false positive rate should be very low
    assert(false_positives < 500); // should be much lower, but let's be safe
}

int main() {
    TestBloomFilter();

    std::cout << "All BloomFilter tests passed!" << std::endl;
    return 0;
}