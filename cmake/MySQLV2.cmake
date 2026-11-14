find_program(SYSBENCH_CARGO cargo REQUIRED)
get_filename_component(SYSBENCH_MYSQL_CLIENT_LIB_DIR
    "${SYSBENCH_MYSQL_CLIENT_LIB_DIR}" ABSOLUTE)
if(NOT EXISTS "${SYSBENCH_MYSQL_CLIENT_LIB_DIR}/Cargo.toml")
    message(FATAL_ERROR "SYSBENCH_MYSQL_CLIENT_LIB_DIR must contain mysql-client-lib/Cargo.toml")
endif()

set(MYSQL_V2_BUILD_DIR "${CMAKE_CURRENT_BINARY_DIR}/mysql-v2")
file(MAKE_DIRECTORY "${MYSQL_V2_BUILD_DIR}/scratch")
configure_file(rust/mysql_v2/Cargo.toml.in "${MYSQL_V2_BUILD_DIR}/Cargo.toml" @ONLY)
configure_file(rust/mysql_v2/Cargo.lock "${MYSQL_V2_BUILD_DIR}/Cargo.lock" COPYONLY)
file(GLOB_RECURSE MYSQL_V2_RUST_SOURCES CONFIGURE_DEPENDS
    "${SYSBENCH_MYSQL_CLIENT_LIB_DIR}/src/*.rs"
    "${CMAKE_CURRENT_SOURCE_DIR}/rust/mysql_v2/src/*.rs")
set(MYSQL_V2_ARCHIVE "${MYSQL_V2_BUILD_DIR}/target/release/libsysbench_mysql_v2.a")
add_custom_command(OUTPUT "${MYSQL_V2_ARCHIVE}"
    COMMAND ${CMAKE_COMMAND} -E env "TMPDIR=${MYSQL_V2_BUILD_DIR}/scratch"
        "${SYSBENCH_CARGO}" build --release --locked
        --manifest-path "${MYSQL_V2_BUILD_DIR}/Cargo.toml"
        --target-dir "${MYSQL_V2_BUILD_DIR}/target"
    DEPENDS ${MYSQL_V2_RUST_SOURCES} "${MYSQL_V2_BUILD_DIR}/Cargo.toml"
        "${MYSQL_V2_BUILD_DIR}/Cargo.lock" "${SYSBENCH_MYSQL_CLIENT_LIB_DIR}/Cargo.toml"
    WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
    COMMENT "Building the Rust MySQL protocol v2 client"
    VERBATIM)
add_custom_target(mysql_v2_rust_build DEPENDS "${MYSQL_V2_ARCHIVE}")
add_library(mysql_v2_rust STATIC IMPORTED)
set_target_properties(mysql_v2_rust PROPERTIES IMPORTED_LOCATION "${MYSQL_V2_ARCHIVE}")
add_dependencies(mysql_v2_rust mysql_v2_rust_build)
target_sources(sysbench_core PRIVATE src/mysql_v2.cpp)
target_compile_definitions(sysbench_core PUBLIC SYSBENCH_HAVE_MYSQL_V2=1)
target_link_libraries(sysbench_core PRIVATE mysql_v2_rust ${CMAKE_DL_LIBS} m)
