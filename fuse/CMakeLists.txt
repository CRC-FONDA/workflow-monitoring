cmake_minimum_required(VERSION 3.18)

set(CMAKE_CXX_STANDARD 20)
project(LogFs)

add_executable(logfs
    src/main.cpp
    src/FileSystem.cpp
    src/LogEntry.cpp
    src/fs.cpp
    src/node.cpp
    src/file.cpp
    src/directory.cpp
    src/link.cpp
)
target_include_directories(logfs PRIVATE inc)
target_link_libraries(logfs fuse3 pthread)
target_compile_definitions(logfs PUBLIC FUSE_USE_VERSION=35)
