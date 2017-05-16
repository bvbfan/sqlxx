sqlxx
=====
C++11 headers only cursors for SQLite / MySQL(MariaDB) / PostreSQL

Motivation:
-----------
  * extremely low-memory-footprint (it depends on result set)
  * blazing fast speed (it depends on intra/inter net connection speed)
  * interact with stl algorithms
  * interact with range-based 'for' loops
  * abstract layer over SQL native backends
  * sql bindings
  * streams
  * easy to use
  * thread safe (with limitations)

You should:
-----------
  * use all database backends at same time or just only one
  * use '?' for binding in all backends, in PostreSQL it's transform to '${\d}'
  * use 'values' helper for bindings, it's std::tuple under the hood
  * use BLOB as sql type in all backend, in PostreSQL it's transform to BYTEA
  * use 'blob' helper for binding blobs
  * use 'value' helper for named bindings (where available), it's std::pair
  * use '{\d}' to build dynamic query only for column names
  * use 'format' helper for '{\d}', it's std::initializer_list< std::string >
  * use std::string for queries only with query contructor
  * use operator<< C-string for queries
  * use operator<< std::string for binding
  * benefit auto escaped \ and '
  * define USE_SHARED_CONNECTION in threaded environment

You should NOT:
---------------
  * use operator<< std::string for queries (it will be bind)
  * use '{\d}' to build dynamic queries except for column names, \ and ' are not escaped
  * share cursor or iterator between threads
  * cache iterators, cursor menage a shared statement, iterator has a weak reference to it
  * allow your cursor/iterator to outlive your database connection i.e.
    auto cursor = sqlitexx::connection::create()->query()->execute();
  * use 'using namespace', different backends shares same names

Test compilation on Linux:
--------------------------
  * you need libmysqlclient/libmariadbclient, sqlite3, postresql-libs (abi+api packages)
  * postgresql, mysql/mariadb server packages
  * start service, create user, database
  * g++ -Wall --std=c++11 -O3 -s test.cpp -o test -lmysqlclient -lsqlite3 -lpq -lpthread
  * -lpthread may needed due to gcc bug

Contributions are welcome
-------------------------
