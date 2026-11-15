cmake_minimum_required(VERSION 3.28)

project(sysbench_cpp
    VERSION 1.0.0
    DESCRIPTION "A low-overhead, C++26 sysbench port with io_uring and coroutines"
    LANGUAGES CXX)

option(SYSBENCH_NATIVE "Build with -march=native" OFF)
option(SYSBENCH_WERROR "Treat compiler warnings as errors" OFF)
option(SYSBENCH_BUILD_TESTS "Build unit and performance tests" ON)
option(SYSBENCH_LAYER_STATS "Account TSC cycles per layer and print a per-query breakdown" OFF)
set(SYSBENCH_MYSQL_CLIENT_LIB_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../mysql-client-lib"
    CACHE PATH "Path to the Rust mysql-client-lib source tree")
# The Rust client is opt-in: benchmarks use the C++ client (protocol 1).
option(SYSBENCH_MYSQL_V2 "Build sql-rust protocol revision 2 using the Rust client" OFF)

set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)
find_package(PkgConfig REQUIRED)
pkg_check_modules(LIBURING REQUIRED IMPORTED_TARGET liburing)
find_package(OpenSSL REQUIRED)
find_library(NUMA_LIBRARY numa REQUIRED)
include(cmake/LuaJIT.cmake)

include(GNUInstallDirs)
set(CMAKE_INSTALL_DOCDIR "${CMAKE_INSTALL_DATAROOTDIR}/doc/sysbench-cpp")
set(SYSBENCH_INSTALL_LUA_DIR "${CMAKE_INSTALL_FULL_DATADIR}/sysbench-cpp/lua")

add_library(sysbench_core STATIC
    src/memory.cpp
    src/numa.cpp
    src/runtime.cpp
    src/io_ring.cpp
    src/socket_io.cpp
    src/db.cpp
    src/stats.cpp
    src/layer_stats.cpp
    src/nic_queues.cpp
    src/options.cpp
    src/mysql_wire.cpp
    src/mysql_connection.cpp
    src/lua_engine.cpp
)

target_include_directories(sysbench_core
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:include>)

target_link_libraries(sysbench_core
    PUBLIC
        Threads::Threads
        PkgConfig::LIBURING
        luajit
        ${NUMA_LIBRARY}
    PRIVATE
        OpenSSL::Crypto
        OpenSSL::SSL)

target_compile_options(sysbench_core PRIVATE
    -Wall -Wextra -Wpedantic -Wconversion -Wshadow
)

if(SYSBENCH_NATIVE)
    target_compile_options(sysbench_core PRIVATE -march=native)
endif()

if(SYSBENCH_WERROR)
    target_compile_options(sysbench_core PRIVATE -Werror)
endif()

target_compile_definitions(sysbench_core PUBLIC
    SYSBENCH_VERSION="${PROJECT_VERSION}"
    SYSBENCH_LUA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/lua"
    SYSBENCH_INSTALL_LUA_DIR="${SYSBENCH_INSTALL_LUA_DIR}")
if(SYSBENCH_LAYER_STATS)
    target_compile_definitions(sysbench_core PUBLIC SYSBENCH_LAYER_STATS=1)
endif()
option(SYSBENCH_TURNAROUND "Report the client turnaround (reply reaped to next submit) per worker" OFF)
if(SYSBENCH_TURNAROUND)
    target_compile_definitions(sysbench_core PUBLIC SYSBENCH_TURNAROUND=1)
endif()

add_executable(sysbench26 src/main.cpp)
# One-connection prepared SELECT 1, one layer at a time (diagnostics).
add_executable(sb_netbench tools/netbench.cpp)
target_link_libraries(sb_netbench PRIVATE sysbench_core)
if(SYSBENCH_MYSQL_V2)
    include(cmake/MySQLV2.cmake)
endif()
target_link_libraries(sysbench26 PRIVATE sysbench_core)
target_compile_options(sysbench26 PRIVATE -Wall -Wextra -Wpedantic -Wshadow)

install(TARGETS sysbench26 RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install(DIRECTORY lua/ DESTINATION ${CMAKE_INSTALL_DATADIR}/sysbench-cpp/lua)
install(FILES README.md NOTICE COPYING DESTINATION ${CMAKE_INSTALL_DOCDIR})
install(DIRECTORY docs/ DESTINATION ${CMAKE_INSTALL_DOCDIR})

if(SYSBENCH_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

# ---------------------------------------------------------------------------
# Distribution packages
# ---------------------------------------------------------------------------

set(CPACK_PACKAGE_NAME "sysbench-cpp")
set(CPACK_PACKAGE_VENDOR "sysbench-cpp")
set(CPACK_PACKAGE_CONTACT "sysbench-cpp maintainers <sysbench-cpp@localhost>")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "${PROJECT_DESCRIPTION}")
set(CPACK_PACKAGE_VERSION "${PROJECT_VERSION}")
set(CPACK_PACKAGE_VERSION_MAJOR "${PROJECT_VERSION_MAJOR}")
set(CPACK_PACKAGE_VERSION_MINOR "${PROJECT_VERSION_MINOR}")
set(CPACK_PACKAGE_VERSION_PATCH "${PROJECT_VERSION_PATCH}")
set(CPACK_PACKAGE_INSTALL_DIRECTORY "sysbench-cpp")
set(CPACK_PACKAGING_INSTALL_PREFIX "/usr")
set(CPACK_OUTPUT_FILE_PREFIX "${CMAKE_BINARY_DIR}/packages")
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/COPYING")
set(CPACK_GENERATOR "DEB;RPM")

# Debian / DPKG
set(CPACK_DEBIAN_PACKAGE_NAME "sysbench-cpp")
set(CPACK_DEBIAN_PACKAGE_MAINTAINER "${CPACK_PACKAGE_CONTACT}")
set(CPACK_DEBIAN_PACKAGE_SECTION "utils")
set(CPACK_DEBIAN_PACKAGE_PRIORITY "optional")
set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
set(CPACK_DEBIAN_FILE_NAME "DEB-DEFAULT")
set(CPACK_DEBIAN_PACKAGE_DESCRIPTION "A low-overhead C++26 sysbench port with io_uring networking, coroutine scheduling, NUMA-local memory and a MySQL wire-protocol client.\nIncludes the sysbench-compatible Lua benchmark interface and bundled OLTP, random-range/point and bulk-insert benchmarks.\n.\nMySQL TLS (off/preferred/required), client certificates and CA/hostname verification are supported.")

# RPM
set(CPACK_RPM_PACKAGE_NAME "sysbench-cpp")
set(CPACK_RPM_PACKAGE_SUMMARY "${PROJECT_DESCRIPTION}")
set(CPACK_RPM_PACKAGE_DESCRIPTION "Low-overhead C++26 sysbench port with io_uring, coroutine scheduling, NUMA-local memory and a MySQL wire-protocol client with TLS. Includes the sysbench-compatible Lua benchmark interface.")
set(CPACK_RPM_PACKAGE_LICENSE "GPL-2.0-or-later")
set(CPACK_RPM_PACKAGE_GROUP "Applications/System")
set(CPACK_RPM_PACKAGE_RELEASE "1")
set(CPACK_RPM_PACKAGE_AUTOREQ ON)
set(CPACK_RPM_PACKAGE_AUTOPROV ON)
set(CPACK_RPM_FILE_NAME "RPM-DEFAULT")

include(CPack)

set(CPACK_COMMAND "${CMAKE_CPACK_COMMAND}")
if(NOT CPACK_COMMAND)
    set(CPACK_COMMAND cpack)
endif()

add_custom_target(package-deb
    COMMAND ${CMAKE_COMMAND} -E make_directory "${CPACK_OUTPUT_FILE_PREFIX}"
    COMMAND ${CPACK_COMMAND} -G DEB --config "${CMAKE_BINARY_DIR}/CPackConfig.cmake"
    DEPENDS sysbench26
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
    COMMENT "Building Debian (.deb) package"
    VERBATIM)

add_custom_target(package-rpm
    COMMAND ${CMAKE_COMMAND} -E make_directory "${CPACK_OUTPUT_FILE_PREFIX}"
    COMMAND ${CPACK_COMMAND} -G RPM --config "${CMAKE_BINARY_DIR}/CPackConfig.cmake"
    DEPENDS sysbench26
    WORKING_DIRECTORY "${CMAKE_BINARY_DIR}"
    COMMENT "Building RPM package"
    VERBATIM)

add_custom_target(deb DEPENDS package-deb)
add_custom_target(dpkg DEPENDS package-deb)
add_custom_target(rpm DEPENDS package-rpm)
add_custom_target(packages DEPENDS package-deb package-rpm)
