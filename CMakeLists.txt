cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME diff_patch)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
# find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)
# Ensure yyjson headers from DuckDB are visible
include_directories(${CMAKE_CURRENT_LIST_DIR}/duckdb/third_party/yyjson/include)

set(EXTENSION_SOURCES src/diff_patch_extension.cpp src/diff_lib.cpp)

# Only build the DuckDB extension targets when included in DuckDB's build system
if (COMMAND build_static_extension)
  build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
  build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})
endif()

# Link OpenSSL in both the static library as the loadable extension
# target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)
# target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto)

if (TARGET ${EXTENSION_NAME})
  install(
    TARGETS ${EXTENSION_NAME}
    EXPORT "${DUCKDB_EXPORT_SET}"
    LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
endif()

