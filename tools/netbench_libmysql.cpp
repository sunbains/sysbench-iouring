// Copyright (C) 2026 Sunny Bains
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.

// netbench's reference: the query loop sysbench's drv_mysql runs
// (mysql_real_query, mysql_store_result, fetch the rows, mysql_free_result)
// with libmysqlclient, one connection, pinned like netbench's worker.
//
// Built by hand against the client library sysbench links:
//   g++ -O2 -I<mysql-server>/include -I<mysql-server>/bld/include \
//       tools/netbench_libmysql.cpp /lib/x86_64-linux-gnu/libmysqlclient.so.24

#include <mysql.h>
#include <sched.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

int main(int argc, char** argv) {
    std::string host = "127.0.0.1", user = "root", password, database;
    unsigned port = 3306;
    double seconds = 5.0;
    int cpu = 0;
    bool prepared = false;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto value = [&](const char* name) -> const char* {
            const std::string prefix = std::string("--") + name + "=";
            return arg.rfind(prefix, 0) == 0 ? argv[i] + prefix.size() : nullptr;
        };
        if (const char* v = value("mysql-host")) host = v;
        else if (const char* v = value("mysql-port")) port = static_cast<unsigned>(std::atoi(v));
        else if (const char* v = value("mysql-user")) user = v;
        else if (const char* v = value("mysql-password")) password = v;
        else if (const char* v = value("mysql-db")) database = v;
        else if (const char* v = value("time")) seconds = std::atof(v);
        else if (const char* v = value("cpu")) cpu = std::atoi(v);
        else if (arg == "--prepared") prepared = true;
        else if (arg == "--text") prepared = false;
    }
    if (cpu >= 0) {
        cpu_set_t set;
        CPU_ZERO(&set);
        CPU_SET(cpu, &set);
        sched_setaffinity(0, sizeof(set), &set);
    }
    MYSQL* con = mysql_init(nullptr);
    unsigned ssl_mode = SSL_MODE_DISABLED;
    mysql_options(con, MYSQL_OPT_SSL_MODE, &ssl_mode);
    if (mysql_real_connect(con, host.c_str(), user.c_str(), password.c_str(), database.c_str(),
                           port, nullptr, 0) == nullptr) {
        std::fprintf(stderr, "connect: %s\n", mysql_error(con));
        return 1;
    }
    MYSQL_STMT* statement = nullptr;
    if (prepared) {
        statement = mysql_stmt_init(con);
        if (mysql_stmt_prepare(statement, "SELECT 1", 8) != 0) {
            std::fprintf(stderr, "prepare: %s\n", mysql_stmt_error(statement));
            return 1;
        }
    }
    auto one = [&]() {
        if (prepared) {
            if (mysql_stmt_execute(statement) != 0 || mysql_stmt_store_result(statement) != 0) std::exit(2);
            while (mysql_stmt_fetch(statement) == 0) {
            }
            mysql_stmt_free_result(statement);
            return;
        }
        if (mysql_real_query(con, "SELECT 1", 8) != 0) std::exit(2);
        MYSQL_RES* result = mysql_store_result(con);
        if (result == nullptr) std::exit(3);
        while (mysql_fetch_row(result) != nullptr) {
        }
        mysql_free_result(result);
    };
    using Clock = std::chrono::steady_clock;
    const auto warm = Clock::now() + std::chrono::milliseconds(500);
    while (Clock::now() < warm) one();
    const auto start = Clock::now();
    unsigned long long queries = 0;
    while (Clock::now() - start < std::chrono::duration<double>(seconds)) {
        for (int i = 0; i < 256; ++i) one();
        queries += 256;
    }
    const double elapsed = std::chrono::duration<double>(Clock::now() - start).count();
    const double qps = static_cast<double>(queries) / elapsed;
    std::printf("%-13s qps %10.0f  %7.2f us/query\n", prepared ? "libmysql-ps" : "libmysql", qps, 1e6 / qps);
    mysql_close(con);
    return 0;
}
