cmake_minimum_required(VERSION 3.15)
project(Ots2ExternalIndexers LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(
    -Wall
    -Wextra
    -pedantic
    $<$<CONFIG:Release>:-O2>
)

# Directories
set(SRC_DIR "${CMAKE_CURRENT_SOURCE_DIR}/src")
set(BIN_DIR "${CMAKE_CURRENT_BINARY_DIR}/bin")

# Place the final executables in <build>/bin/
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${BIN_DIR}")

find_package(PkgConfig REQUIRED)
pkg_check_modules(CPPHTTPLIB REQUIRED cpp-httplib)
# Wrap the pkg-config result in an imported interface target
add_library(cpp-httplib::cpp-httplib INTERFACE IMPORTED)
set_target_properties(cpp-httplib::cpp-httplib PROPERTIES
    INTERFACE_INCLUDE_DIRECTORIES "${CPPHTTPLIB_INCLUDE_DIRS}"
    INTERFACE_LINK_LIBRARIES      "${CPPHTTPLIB_LIBRARIES}"
)

find_package(CLI11 REQUIRED)
find_package(CURL REQUIRED)
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)

# Library sources
file(GLOB_RECURSE LIB_SOURCES CONFIGURE_DEPENDS "${SRC_DIR}/*.cpp")
# Remove the entry points
list(REMOVE_ITEM LIB_SOURCES
    "${SRC_DIR}/main.cpp"
    "${SRC_DIR}/fetch_events.cpp"
    "${SRC_DIR}/rpc_server.cpp"
    "${SRC_DIR}/indexer.cpp")

set(LIB_SRCS ${LIB_SOURCES})

function(add_event_target TARGET_NAME MAIN_SRC)
    add_executable(${TARGET_NAME}
        ${MAIN_SRC}
        ${LIB_SRCS}
    )
    
    target_include_directories(${TARGET_NAME} PUBLIC ${CPPHTTPLIB_INCLUDE_DIRS})

    # Link against external libraries
    target_link_libraries(${TARGET_NAME}
        PRIVATE
            CURL::libcurl
            leveldb
            Threads::Threads
            OpenSSL::SSL
            OpenSSL::Crypto
        PUBLIC
            cpp-httplib::cpp-httplib
            CLI11::CLI11
    )
endfunction()

# Define the three programs
add_event_target(event_syncer "${SRC_DIR}/main.cpp")
add_event_target(fetch_events "${SRC_DIR}/fetch_events.cpp")
add_event_target(rpc_server "${SRC_DIR}/rpc_server.cpp")
add_event_target(indexer "${SRC_DIR}/indexer.cpp")
