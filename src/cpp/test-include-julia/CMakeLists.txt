cmake_minimum_required(VERSION 3.2.0)

project(TestLib)

include(ExternalProject)
include(FindPkgConfig)
include(GNUInstallDirs)

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")

set(CMAKE_MACOSX_RPATH 1)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/lib")

find_package(JlCxx)
get_target_property(JlCxx_location JlCxx::cxxwrap_julia LOCATION)
get_filename_component(JlCxx_location ${JlCxx_location} DIRECTORY)
set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib;${JlCxx_location}")

message(STATUS "Found JlCxx at ${JlCxx_location}")

# This ensures that things like gnu++11 get passed correctly
set(CMAKE_CXX_STANDARD 11)

# We require a C++11 compliant compiler
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(Arrow)
find_package(Parquet)

include_directories(SYSTEM ${ARROW_INCLUDE_DIR} ${PARQUET_INCLUDE_DIR})

add_library(testlib SHARED testlib.cpp)

target_include_directories(testlib PUBLIC /home/xiaodai/git/arrow_install_path/include)

target_link_libraries(testlib JlCxx::cxxwrap_julia ${PARQUET_SHARED_LIB} ${ARROW_SHARED_LIB})

install(TARGETS
  testlib
LIBRARY DESTINATION lib
ARCHIVE DESTINATION lib
RUNTIME DESTINATION lib)