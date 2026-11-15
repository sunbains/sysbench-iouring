# LuaJIT, vendored from sysbench (third_party/luajit, LuaJIT 2.1.0-beta3)
# and linked statically.
#
# LuaJIT's make builds in place, so the sources are copied into the build tree
# and built there: the source tree stays clean.
#
#   LUAJIT_ENABLE_GC64      64-bit GC references.  Needed for custom
#                           allocators (each connection's Lua heap is one).
#   LUAJIT_UNWIND_EXTERNAL  DWARF unwinding, so C++ exceptions can cross
#                           Lua frames.

include(ExternalProject)

set(SYSBENCH_LUAJIT_SOURCE "${CMAKE_CURRENT_SOURCE_DIR}/third_party/luajit")
set(SYSBENCH_LUAJIT_WORK "${CMAKE_BINARY_DIR}/third_party/luajit")
set(SYSBENCH_LUAJIT_LIBRARY "${SYSBENCH_LUAJIT_WORK}/src/libluajit.a")

find_program(SYSBENCH_MAKE NAMES gmake make REQUIRED)

ExternalProject_Add(luajit_build
    DOWNLOAD_COMMAND
        ${CMAKE_COMMAND} -E copy_directory "${SYSBENCH_LUAJIT_SOURCE}" "${SYSBENCH_LUAJIT_WORK}"
    SOURCE_DIR "${SYSBENCH_LUAJIT_WORK}"
    BUILD_IN_SOURCE TRUE
    CONFIGURE_COMMAND ""
    BUILD_COMMAND
        ${SYSBENCH_MAKE} -C src -j8 BUILDMODE=static
            "XCFLAGS=-DLUAJIT_ENABLE_GC64 -DLUAJIT_UNWIND_EXTERNAL"
            "CFLAGS=-O2 -fno-omit-frame-pointer -funwind-tables"
            libluajit.a
    INSTALL_COMMAND ""
    BUILD_BYPRODUCTS "${SYSBENCH_LUAJIT_LIBRARY}"
    LOG_BUILD TRUE
    LOG_OUTPUT_ON_FAILURE TRUE)

add_library(luajit STATIC IMPORTED GLOBAL)
set_target_properties(luajit PROPERTIES
    IMPORTED_LOCATION "${SYSBENCH_LUAJIT_LIBRARY}"
    # The public headers (lua.h, lauxlib.h, lualib.h, luaconf.h, luajit.h) are
    # plain sources: use them from the vendored tree.
    INTERFACE_INCLUDE_DIRECTORIES "${SYSBENCH_LUAJIT_SOURCE}/src"
    INTERFACE_LINK_LIBRARIES "${CMAKE_DL_LIBS};m")
add_dependencies(luajit luajit_build)
