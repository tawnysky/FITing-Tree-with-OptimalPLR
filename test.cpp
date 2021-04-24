
#pragma once

#include <algorithm>
#include <fstream>

#include "include/zipfian.h"
#include "include/zipfian_random.h"
#include "include/inplace_index.h"
#include "include/buffer_index.h"

using K = double;
using P = int64_t;
using TestIndex = BufferIndex<K, P>;

void YCSB_A(const size_t number, const K* random_keys, TestIndex& test_index, std::ifstream& file) {
    size_t transactions = 2e8;
    size_t times = transactions >> 1;
    std::string str;

    std::cout<<"Preparing workloads..."<<std::endl;
    K* search_keys = new K[times];
    auto* zipfian = new Zipfian(number);
    for (size_t i = 0; i < times; i++) {
        search_keys[i] = random_keys[zipfian->next()];
    }
    delete zipfian;
    std::uniform_int_distribution<P> payload_dist(-1e9,1e9);
    std::mt19937 payload_gen(time(NULL));
    K* new_keys = new K[times];
    P* new_payloads = new P[times];
    for (size_t i = 0; i < times; i++) {
        getline(file, str);
        new_keys[i] = atof(str.c_str());
        new_payloads[i] = payload_dist(payload_gen);
    }
    timeval t1, t2;
    P* search_answers = new P[times];

    std::cout<<"Warming up..."<<std::endl;
    std::uniform_int_distribution<size_t> dist_temp(0, number - 1);
    std::mt19937 gen_temp(time(NULL));
    for (size_t i = 0; i < 1e8; i++) {
        test_index.find(random_keys[dist_temp(gen_temp)]);
    }

    std::cout<<"Start doing YCSB-A benchmark..."<<std::endl;
    gettimeofday(&t1, NULL);
    for(size_t i = 0; i < times; i++){
        search_answers[i] = test_index.find(search_keys[i]);
        test_index.upsert(new_keys[i], new_payloads[i]);
    }
    gettimeofday(&t2, NULL);
    std::cout<<"Total time: "<<(t2.tv_sec*1e6+t2.tv_usec-t1.tv_sec*1e6-t1.tv_usec)/1000<<" ms"<<std::endl;
    delete [] search_keys;
    delete [] search_answers;
    delete [] new_keys;
    delete [] new_payloads;
    std::cout<<"YCSB-A benchmark complete!"<<std::endl;
}

void YCSB_B(const size_t number, const K* random_keys, TestIndex& test_index, std::ifstream& file) {
    size_t transactions = 2e8;
    size_t insert_times = transactions / 20;
    size_t search_times = transactions - insert_times;
    std::string str;

    std::cout<<"Preparing workloads..."<<std::endl;
    K* search_keys = new K[search_times];
    auto* zipfian = new Zipfian(number);
    for (size_t i = 0; i < search_times; i++) {
        search_keys[i] = random_keys[zipfian->next()];
    }
    delete zipfian;
    std::uniform_int_distribution<P> payload_dist(-1e9,1e9);
    std::mt19937 payload_gen(time(NULL));
    K* new_keys = new K[insert_times];
    P* new_payloads = new P[insert_times];
    for (size_t i = 0; i < insert_times; i++) {
        getline(file, str);
        new_keys[i] = atof(str.c_str());
        new_payloads[i] = payload_dist(payload_gen);
    }
    timeval t1, t2;
    P* search_answers = new P[search_times];

    std::cout<<"Warming up..."<<std::endl;
    std::uniform_int_distribution<size_t> dist_temp(0, number - 1);
    std::mt19937 gen_temp(time(NULL));
    for (size_t i = 0; i < 1e8; i++) {
        test_index.find(random_keys[dist_temp(gen_temp)]);
    }

    std::cout<<"Start doing YCSB-B benchmark..."<<std::endl;
    gettimeofday(&t1, NULL);
    for(size_t i = 0; i < insert_times; i++){
        for (size_t j = i * 19; j < (i + 1) * 19; j++)
            search_answers[j] = test_index.find(search_keys[j]);
        test_index.upsert(new_keys[i], new_payloads[i]);
    }
    gettimeofday(&t2, NULL);
    std::cout<<"Total time: "<<(t2.tv_sec*1e6+t2.tv_usec-t1.tv_sec*1e6-t1.tv_usec)/1000<<" ms"<<std::endl;
    delete [] search_keys;
    delete [] search_answers;
    delete [] new_keys;
    delete [] new_payloads;
    std::cout<<"YCSB-B benchmark complete!"<<std::endl;
}

void YCSB_C(const size_t number, const K* random_keys, TestIndex& test_index) {
    size_t transactions = 2e8;
    size_t times = transactions;

    std::cout<<"Preparing workloads..."<<std::endl;
    K* search_keys = new K[times];
    auto* zipfian = new Zipfian(number);
    for (size_t i = 0; i < times; i++) {
        search_keys[i] = random_keys[zipfian->next()];
    }
    delete zipfian;
    timeval t1, t2;
    P* search_answers = new P[times];

    std::cout<<"Warming up..."<<std::endl;
    std::uniform_int_distribution<size_t> dist_temp(0, number - 1);
    std::mt19937 gen_temp(time(NULL));
    for (size_t i = 0; i < 1e8; i++) {
        test_index.find(random_keys[dist_temp(gen_temp)]);
    }

    std::cout<<"Start doing YCSB-C benchmark..."<<std::endl;
    gettimeofday(&t1, NULL);
    for(size_t i = 0; i < times; i++){
        search_answers[i] = test_index.find(search_keys[i]);
    }
    gettimeofday(&t2, NULL);
    std::cout<<"Total time: "<<(t2.tv_sec*1e6+t2.tv_usec-t1.tv_sec*1e6-t1.tv_usec)/1000<<" ms"<<std::endl;
    delete [] search_keys;
    delete [] search_answers;
    std::cout<<"YCSB-C benchmark complete!"<<std::endl;
}

void YCSB_D(const size_t number, const K* random_keys, TestIndex& test_index, std::ifstream& file) {
    size_t transactions = 2e8;
    size_t times = transactions / 20;
    std::string str;

    std::cout<<"Preparing workloads..."<<std::endl;
    std::uniform_int_distribution<P> payload_dist(-1e9,1e9);
    std::mt19937 payload_gen(time(NULL));
    K* new_keys = new K[times];
    P* new_payloads = new P[times];
    for (size_t i = 0; i < times; i++) {
        getline(file, str);
        new_keys[i] = atof(str.c_str());
        new_payloads[i] = payload_dist(payload_gen);
    }
    timeval t1, t2;
    P* search_answers = new P[transactions - times];

    std::cout<<"Warming up..."<<std::endl;
    std::uniform_int_distribution<size_t> dist_temp(0, number - 1);
    std::mt19937 gen_temp(time(NULL));
    for (size_t i = 0; i < 1e8; i++) {
        test_index.find(random_keys[dist_temp(gen_temp)]);
    }

    std::cout<<"Start doing YCSB-D benchmark..."<<std::endl;
    gettimeofday(&t1, NULL);
    for(size_t i = 0; i < times; i++){
        test_index.upsert(new_keys[i], new_payloads[i]);
        for (size_t j = 0; j < 19; j++)
            search_answers[i * 19 + j] = test_index.find(new_keys[i]);
    }
    gettimeofday(&t2, NULL);
    std::cout<<"Total time: "<<(t2.tv_sec*1e6+t2.tv_usec-t1.tv_sec*1e6-t1.tv_usec)/1000<<" ms"<<std::endl;
    delete [] search_answers;
    delete [] new_keys;
    delete [] new_payloads;
    std::cout<<"YCSB-D benchmark complete!"<<std::endl;
}

void YCSB_E(const size_t number, const K* ordered_keys, TestIndex& test_index, std::ifstream& file) {
    size_t transactions = 2e8;
    size_t insert_times = transactions / 20;
    size_t scan_times = transactions - insert_times;
    std::string str;

    std::cout<<"Preparing workloads..."<<std::endl;
    K* lower_bounds = new K[scan_times];
    K* upper_bounds = new K[scan_times];
    auto* zipfian_random = new ZipfianRandom(number);
    for (size_t i = 0; i < scan_times; i++) {
        size_t lower_pos = zipfian_random->next();
        size_t upper_pos = std::min(lower_pos + i % 100, number - 1);
        lower_bounds[i] = ordered_keys[lower_pos];
        upper_bounds[i] = ordered_keys[upper_pos];
    }
    delete zipfian_random;
    std::uniform_int_distribution<P> payload_dist(-1e9,1e9);
    std::mt19937 payload_gen(time(NULL));
    K* new_keys = new K[insert_times];
    P* new_payloads = new P[insert_times];
    for (size_t i = 0; i < insert_times; i++) {
        getline(file, str);
        new_keys[i] = atof(str.c_str());
        new_payloads[i] = payload_dist(payload_gen);
    }
    timeval t1, t2;

    std::cout<<"Warming up..."<<std::endl;
    std::uniform_int_distribution<size_t> dist_temp(0, number - 1);
    std::mt19937 gen_temp(time(NULL));
    for (size_t i = 0; i < 1e8; i++) {
        test_index.find(ordered_keys[dist_temp(gen_temp)]);
    }

    std::cout<<"Start doing YCSB-E benchmark..."<<std::endl;
    gettimeofday(&t1, NULL);
    for(size_t i = 0; i < insert_times; i++){
        for (size_t j = i * 19; j < (i + 1) * 19; j++) {
            std::vector<std::pair<K, P>> answers;
            answers.reserve(100);
            test_index.range_query(lower_bounds[j], upper_bounds[j], answers);
        }
        test_index.upsert(new_keys[i], new_payloads[i]);
    }
    gettimeofday(&t2, NULL);
    std::cout<<"Total time: "<<(t2.tv_sec*1e6+t2.tv_usec-t1.tv_sec*1e6-t1.tv_usec)/1000<<" ms"<<std::endl;
    delete [] lower_bounds;
    delete [] upper_bounds;
    delete [] new_keys;
    delete [] new_payloads;
    std::cout<<"YCSB-E benchmark complete!"<<std::endl;
}

void YCSB_F(const size_t number, const K* random_keys, TestIndex& test_index) {
    size_t transactions = 2e8;
    size_t times = transactions >> 1;

    std::cout<<"Preparing workloads..."<<std::endl;
    K* search_keys = new K[times];
    auto* zipfian = new Zipfian(number);
    for (size_t i = 0; i < times; i++) {
        search_keys[i] = random_keys[zipfian->next()];
    }
    delete zipfian;
    std::uniform_int_distribution<P> payload_dist(-1e9,1e9);
    std::mt19937 payload_gen(time(NULL));
    P* new_payloads = new P[times];
    for (size_t i = 0; i < times; i++) {
        new_payloads[i] = payload_dist(payload_gen);
    }
    timeval t1, t2;
    P* search_answers = new P[times];

    std::cout<<"Warming up..."<<std::endl;
    std::uniform_int_distribution<size_t> dist_temp(0, number - 1);
    std::mt19937 gen_temp(time(NULL));
    for (size_t i = 0; i < 1e8; i++) {
        test_index.find(random_keys[dist_temp(gen_temp)]);
    }

    std::cout<<"Start doing YCSB-F benchmark..."<<std::endl;
    gettimeofday(&t1, NULL);
    for(size_t i = 0; i < times; i++){
        search_answers[i] = test_index.find(search_keys[i]);
        test_index.upsert(search_keys[i], new_payloads[i]);
    }
    gettimeofday(&t2, NULL);
    std::cout<<"Total time: "<<(t2.tv_sec*1e6+t2.tv_usec-t1.tv_sec*1e6-t1.tv_usec)/1000<<" ms"<<std::endl;
    delete [] search_keys;
    delete [] search_answers;
    delete [] new_payloads;
    std::cout<<"YCSB-F benchmark complete!"<<std::endl;
}

void benchmark() {

    std::ifstream file;
    file.open("/home/tawnysky/osm/normal.data");
    std::string str;

    size_t number = 1e8;

    std::uniform_int_distribution<P> payload_dist(-1e9,1e9);
    std::mt19937 payload_gen(time(NULL));
    K* keys = new K[number];
    P* payloads = new P[number];
    timeval t1, t2;

    // Generate keys and payloads
    for(size_t i = 0; i < number; i++){
        getline(file, str);
        keys[i] = atof(str.c_str());
        payloads[i] = payload_dist(payload_gen);
    }
    K* random_keys = new K[number];
    memcpy(random_keys, keys, number * sizeof(K));

    // Sort keys
    std::sort(keys, keys+number);

    // Build index
    std::cout<<"Start building index..."<<std::endl;
    gettimeofday(&t1, NULL);
    TestIndex test_index(keys, payloads, number);
    gettimeofday(&t2, NULL);
    std::cout<<"Total build time: "<<(t2.tv_sec*1e6+t2.tv_usec-t1.tv_sec*1e6-t1.tv_usec)/1000<<" ms"<<std::endl;
    std::cout<<"Build index complete!"<<std::endl;


//    YCSB_A(number, random_keys, test_index, file);
//    YCSB_B(number, random_keys, test_index, file);
//    YCSB_C(number, random_keys, test_index);
//    YCSB_D(number, random_keys, test_index, file);
//    YCSB_E(number, keys, test_index, file);
//    YCSB_F(number, random_keys, test_index);


    // Upsert benchmark
    size_t upsert_times = 1e8;
    K* new_keys = new K[upsert_times];
    P* new_payloads = new P[upsert_times];
    for(size_t i = 0; i < upsert_times; i++){
        getline(file, str);
        new_keys[i] = atof(str.c_str());
        new_payloads[i] = payload_dist(payload_gen);
    }

    std::cout<<"Warming up..."<<std::endl;
    std::uniform_int_distribution<size_t> dist_temp(0, number - 1);
    std::mt19937 gen_temp(time(NULL));
    for (size_t i = 0; i < 1e8; i++) {
        test_index.find(random_keys[dist_temp(gen_temp)]);
    }

    std::cout<<"Start doing upsert benchmark..."<<std::endl;
    gettimeofday(&t1, NULL);
    for(size_t i = 0; i < upsert_times; i++){
        test_index.upsert(new_keys[i], new_payloads[i]);
    }
    gettimeofday(&t2, NULL);
    std::cout<<"Total upsert time: "<<(t2.tv_sec*1e6+t2.tv_usec-t1.tv_sec*1e6-t1.tv_usec)/1000<<" ms"<<std::endl;
    std::cout<<"Upsert benchmark complete!"<<std::endl;


    // Index info
    test_index.index_size();

}

int main() {
    benchmark();
    return 0;
}
