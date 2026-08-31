cmake_minimum_required(VERSION 3.16)

project(
    EquipmentMonitoringSystem
    VERSION 1.0
    LANGUAGES CXX
)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

# Server
add_executable(
    server
    src/server.cpp
)

target_link_libraries(
    server
    PRIVATE
    Threads::Threads
)

# Device simulator
add_executable(
    device
    src/device.cpp
)

target_link_libraries(
    device
    PRIVATE
    Threads::Threads
)

# Compiler warnings
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(
        server
        PRIVATE
        -Wall
        -Wextra
        -Wpedantic
    )

    target_compile_options(
        device
        PRIVATE
        -Wall
        -Wextra
        -Wpedantic
    )
endif()