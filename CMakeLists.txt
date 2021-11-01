cmake_minimum_required(VERSION 3.16)
project(distributed C)

set(CMAKE_C_STANDARD 99)

add_executable(distributed main.c common.h ipc.h pa1.h)
