cmake_minimum_required(VERSION 3.16)
project(distributed C)

set(CMAKE_C_STANDARD 99)

add_compile_options(-std=c99 -Wall -pedantic)
SET(LINK_FLAGS "-L /mnt/c/Users/mprosolovich/CLionProjects/distributed_calculations_course -lruntime")
SET(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${LINK_FLAGS}")

add_executable(distributed
        pa23.c
        banking.h
        common.h
        ipc.h
        pa2345.h
        )
