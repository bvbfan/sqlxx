
#define USE_SHARED_CONNECTION
#include "mysqlxx.h"
#include "pqsqlxx.h"
#include "sqlitexx.h"

#include <algorithm>
#include <iostream>
#include <thread>

void usage() {
    std::cout << "options: SQLITE|MYSQL|PQSQL\n";
    std::cout << "sub options: SQLITE {db}|MYSQL {host, user, pass, db}|PQSQL {conninfo}\n";
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage();
        return 1;
    }
    const std::string type = argv[1];
    if (!((type == "SQLITE" && argc == 3)
    ||    (type == "MYSQL" && argc == 6)
    ||    (type == "PQSQL" && argc == 3))) {
        usage();
        return 1;
    }
    auto db_connect = [&]() {
        if (type == "SQLITE") {
            return sqlitexx::connection::create(argv[2]);
        } else if (type == "PQSQL") {
            return pqsqlxx::connection::create(argv[2]);
        } else {
            return mysqlxx::connection::create(argv[2], argv[3], argv[4], argv[5]);
        }
    };
    auto con = db_connect();
    if (!con) {
        std::cout << "Can't connect to" << argv[1] << std::endl;
        return 1;
    }
    std::cout << con->version() << std::endl;
    con->query("CREATE TABLE test(name TEXT, iint INTEGER, flo FLOAT, data BLOB);")->execute();
    std::thread *workers[16];
    for (auto &worker : workers) {
        worker = new std::thread([&con]() {
            std::stringstream s;
            s << std::this_thread::get_id();
            std::int64_t i64; s >> i64;
            auto q = con->query("INSERT INTO test ({0}, {1}, {2}, {3}) VALUES (?, ?, ?, ?);");
            (*q) << format{"name", "iint", "flo", "data"}
                 << values("aaaa", int(i64), 5.64f, blob(10, 0x92));
            q->execute();
            q = con->query("SELECT * from test");
            auto cursor = q->execute();
            auto row = *std::find_if(cursor.begin(), cursor.end(), [](sqlxx::row &row) {
               return row["flo"] == 5.64f;
            });
            std::cout << row["name"].toString() << std::endl;
            sqlxx::result result;
            std::for_each(cursor.begin(), cursor.end(), [&result](sqlxx::row &row) {
               result += row;
            });
            std::cout << result.size() << std::endl;
        });
    }
    for (auto &worker : workers) {
        worker->join();
        delete worker;
    }
    return 0;
}
