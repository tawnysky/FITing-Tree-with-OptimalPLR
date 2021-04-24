cmake_minimum_required(VERSION 3.16)
project(FITing_Tree)

set(CMAKE_CXX_STANDARD 17)

set(CMAKE_CXX_FLAGS "-O3 -march=native")

add_executable(FITing_Tree test.cpp include/buffer_index.h include/buffer_node.h include/inplace_index.h include/inplace_node.h include/piecewise_linear_model.h include/btree.h include/parameters.h include/zipfian.h include/zipfian_random.h)
