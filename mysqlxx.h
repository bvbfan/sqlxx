///////////////////////////////////////////////////////////////////////////////
/// \author (c) Anthony Fieroni (bvbfan@abv.bg)
///             2017, Plovdiv, Bulgaria
///
/// \license The MIT License (MIT)
/// Permission is hereby granted, free of charge, to any person obtaining a copy
/// of this software and associated documentation files (the "Software"), to deal
/// in the Software without restriction, including without limitation the rights
/// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
/// copies of the Software, and to permit persons to whom the Software is
/// furnished to do so, subject to the following conditions:
///
/// The above copyright notice and this permission notice shall be included in
/// all copies or substantial portions of the Software.
///
/// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
/// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
/// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
/// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
/// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
/// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
/// THE SOFTWARE.
///////////////////////////////////////////////////////////////////////////////

#ifndef _MYSQLXX_H_
#define _MYSQLXX_H_

#include "sqlxx.h"

#include <mysql/mysql.h>
#include <mysql/errmsg.h>

namespace mysqlxx {

/*
 * Database class
 */
class db
{
public:
  db(std::string const& name) : db_(nullptr), name_(name), open_(false) {}
  ~db() { close(); }

  db(db&&) = delete;            // no move
  db(db const&) = delete;       // no copy
  db& operator=(db&&) = delete; // no assignment
  db& operator=(db const&) = delete; // no assignment

  // MySQL access
#ifdef USE_SHARED_CONNECTION
  sqlxx::connection_lock<::MYSQL> operator()() const {
    return { mutex_, db_ };
  }
#else
  inline ::MYSQL* operator()() const { return db_; }
#endif

  // open (connect) the database
  bool open(char const* host, char const* user, char const* pass, char const* name = nullptr) {
#ifdef USE_SHARED_CONNECTION
    sqlxx::connection_lock<::MYSQL> lock(mutex_, db_);
#endif
    static library_init init;
    if (open_) return true;
    db_ = ::mysql_init(nullptr);
    if (!name) name = name_.c_str();
    else name_ = name;
    open_ = !!db_;
    if (open_) {
      ::my_bool reconnect = 1; unsigned long trunc = 0;
      ::mysql_options(db_, MYSQL_OPT_RECONNECT, &reconnect);
      ::mysql_options(db_, MYSQL_REPORT_DATA_TRUNCATION, &trunc);
    }
    if (db_ && (!::mysql_real_connect(db_, host, user, pass, name, 0, nullptr, 0)
    || ::mysql_query(db_, std::string("use " + name_ + ';').c_str()) != 0)) {
      ::mysql_close(db_);
      open_ = false;
      db_   = nullptr;
    }
    return open_;
  }

  // close the database
  void close() {
#ifdef USE_SHARED_CONNECTION
    sqlxx::connection_lock<::MYSQL> lock(mutex_, db_);
#endif
    if (!open_) return;
    ::mysql_close(db_);
    open_ = false;
    db_   = nullptr;
  }

  // returns true if the database is open
  inline bool is_open() const { return open_; }

  // MySQL version
  inline std::string version() { return std::string(MYSQL_BASE_VERSION); }

  // database defragmentation
  void vacuum() {
    std::string query("SELECT Concat('OPTIMIZE TABLE ',TABLE_NAME, ';') ");
    query += "FROM INFORMATION_SCHEMA.TABLES WHERE table_schema='" + name_ + '\'';
    ::mysql_query((*this)(), query.c_str());
  }

private:
  struct library_init {
    library_init() { ::mysql_library_init(0, nullptr, nullptr); }
    ~library_init() { ::mysql_library_end(); }
  };
  ::MYSQL*          db_;    // associated db
  std::string       name_;  // db name
  bool              open_;  // db open status
#ifdef USE_SHARED_CONNECTION
  mutable std::mutex mutex_;
#endif
};

class statement : public sqlxx::statement {
public:
  statement(db const& db, ::MYSQL_STMT* stmt) : db_(db), res_(nullptr), stmt_(stmt) {
#ifdef USE_SHARED_CONNECTION
    auto&& lock = db_();
#endif
    switch(::mysql_stmt_errno(stmt_)) {
      case 0: result_ = SQL_OK; break;
      case CR_COMMANDS_OUT_OF_SYNC: result_ = SQL_IMPROPER; return;
      case CR_OUT_OF_MEMORY: result_ = SQL_NO_MEMORY; return;
      case CR_SERVER_GONE_ERROR:
      case CR_SERVER_LOST: result_ = SQL_SERVER_LOST; return;
      case CR_UNKNOWN_ERROR:
      default: result_ = SQL_UNKNOWN_ERROR; return;
    }
    if ((res_ = ::mysql_stmt_result_metadata(stmt_))) {
      num_ = ::mysql_num_fields(res_);
    }
    last_id_ = ::mysql_stmt_insert_id(stmt_);
    affected_rows_ = ::mysql_stmt_affected_rows(stmt_);
    std::int64_t(affected_rows_) < 0 && (affected_rows_ = 0);
  }

  statement(statement&&) = delete;
  statement(statement const&) = delete;
  statement& operator=(statement&&) = delete;
  statement& operator=(statement const&) = delete;

  ~statement() override {
#ifdef USE_SHARED_CONNECTION
    auto&& lock = db_();
#endif
    if (res_) ::mysql_free_result(res_);
    ::mysql_stmt_close(stmt_);
  }

  sqlxx::row next() override {
    if (!res_ || !num_) return {};
    sqlxx::row row;
    std::vector<MYSQL_BIND> mbinds(num_);
    for(auto &bind : mbinds) {
      bind.length = &bind.buffer_length;
    }
#ifdef USE_SHARED_CONNECTION
    auto&& lock = db_();
#endif
    ::mysql_stmt_bind_result(stmt_, mbinds.data());
    int res = ::mysql_stmt_fetch(stmt_);
    if (res == 1 || res == MYSQL_NO_DATA) return {};
    for (size_t i = 0; i < num_; ++i) {
      auto &bind = mbinds[i];
      auto field = ::mysql_fetch_field_direct(res_, i);
      switch (field->type)
      {
      case MYSQL_TYPE_TINY: {
        char i8 = 0;
        bind.buffer_type = field->type;
        bind.buffer = reinterpret_cast<void *>(&i8);
        ::mysql_stmt_fetch_column(stmt_, &bind, i, 0);
        row.emplace_back(std::int64_t(i8), field->org_name);
      } break;
      case MYSQL_TYPE_SHORT: {
        short i16 = 0;
        bind.buffer_type = field->type;
        bind.buffer = reinterpret_cast<void *>(&i16);
        ::mysql_stmt_fetch_column(stmt_, &bind, i, 0);
        row.emplace_back(std::int64_t(i16), field->org_name);
      } break;
      case MYSQL_TYPE_INT24:
      case MYSQL_TYPE_LONG: {
        int i32 = 0;
        bind.buffer_type = field->type;
        bind.buffer = reinterpret_cast<void *>(&i32);
        ::mysql_stmt_fetch_column(stmt_, &bind, i, 0);
        row.emplace_back(std::int64_t(i32), field->org_name);
      } break;
      case MYSQL_TYPE_LONGLONG: {
        std::int64_t i64 = 0;
        bind.buffer_type = field->type;
        bind.buffer = reinterpret_cast<void *>(&i64);
        ::mysql_stmt_fetch_column(stmt_, &bind, i, 0);
        row.emplace_back(i64, field->org_name);
      } break;
      case MYSQL_TYPE_FLOAT: {
        float f;
        bind.buffer_type = field->type;
        bind.buffer = reinterpret_cast<void *>(&f);
        ::mysql_stmt_fetch_column(stmt_, &bind, i, 0);
        row.emplace_back(double(f), field->org_name);
      } break;
      case MYSQL_TYPE_DOUBLE: {
        double d;
        bind.buffer_type = field->type;
        bind.buffer = reinterpret_cast<void *>(&d);
        ::mysql_stmt_fetch_column(stmt_, &bind, i, 0);
        row.emplace_back(d, field->org_name);
      } break;
      case MYSQL_TYPE_STRING: case MYSQL_TYPE_VAR_STRING:
      case MYSQL_TYPE_BLOB: if (field->charsetnr == 63) {
        blob v(bind.buffer_length);
        bind.buffer = const_cast<std::uint8_t *>(v.data());
        ::mysql_stmt_fetch_column(stmt_, &bind, i, 0);
        row.emplace_back(std::move(v), field->org_name);
      } else {
        std::string s(bind.buffer_length, '\0');
        bind.buffer = const_cast<char *>(s.data());
        ::mysql_stmt_fetch_column(stmt_, &bind, i, 0);
        row.emplace_back(std::move(s), field->org_name);
      } break;
      case MYSQL_TYPE_NULL:
        row.emplace_back(field->org_name);
        break;
      default:
        row.emplace_back(std::int64_t(0), field->org_name);
        break;
      }
    }
    return row;
  }

  void first() override {
#ifdef USE_SHARED_CONNECTION
    auto&& lock = db_();
#endif
    ::mysql_stmt_data_seek(stmt_, 0);
  }

  result_type result() const override { return result_; };
  std::uint64_t last_id() const override { return last_id_; };
  std::uint64_t affected_rows() const override { return affected_rows_; };

private:
  db const& db_;
  size_t num_ = 0;
  ::MYSQL_RES* res_;
  ::MYSQL_STMT* stmt_;
  result_type result_;
  std::uint64_t last_id_ = 0;
  std::uint64_t affected_rows_ = 0;
};

/*
 * Representation of a transaction
 */
class transaction {
public:
  transaction(::MYSQL* db) : db_(db) {
    finished_ = !begin();
  }

  transaction(transaction&& t) : db_(t.db_), finished_(t.finished_) {
    t.finished_ = true;
  }

  ~transaction() { rollback(); }

  transaction(transaction const&) = delete;
  transaction& operator=(transaction&&) = delete;
  transaction& operator=(transaction const&) = delete;

  bool begin() {
    int err = ::mysql_query(db_, "BEGIN;");
    return err == 0;
  }

  bool commit() {
    if (finished_) return true;
    int err = ::mysql_query(db_, "COMMIT;");
    finished_ = err == 0;
    return finished_;
  }

  bool rollback() {
    if (finished_) return true;
    int err = ::mysql_query(db_, "ROLLBACK;");
    finished_ = err == 0;
    return finished_;
  }

private:
  ::MYSQL* db_;
  bool finished_;
};

class query : public sqlxx::query {
public:
  query(db&& db) = delete;
  query(db const& db) : db_(db) {}
  query(db const& db, std::string const& str) : sqlxx::query(str), db_(db) {}

private:
  int do_bind(::MYSQL_STMT* stmt, std::vector<sqlxx::field_type> binds) {
    auto cnt = ::mysql_stmt_param_count(stmt);
    if (!cnt) return ::mysql_stmt_execute(stmt);
    std::vector<MYSQL_BIND> mbinds(cnt);
    for (size_t i = 0; i < cnt; ++i) {
      if (i >= binds.size()) continue;
      auto &mbind = mbinds[i];
      auto const& bind = binds[i];
      if (bind.type() == SQL_BLOB) {
        std::string const& v = bind;
        mbind.buffer_type = MYSQL_TYPE_BLOB;
        mbind.buffer = const_cast<char *>(v.data());
        mbind.buffer_length = v.size();
        mbind.is_unsigned = static_cast<::my_bool>(1);
      }
      else if (bind.type() == SQL_TEXT) {
        std::string const& s = bind;
        mbind.buffer_type = MYSQL_TYPE_STRING;
        mbind.buffer = const_cast<char *>(s.data());
        mbind.buffer_length = s.size();
      }
      else if (bind.type() == SQL_NULL) {
        mbind.buffer_type = MYSQL_TYPE_NULL;
      }
      else if (bind.type() == SQL_INTEGER) {
        std::int64_t const& i64 = bind;
        if (i64 > std::numeric_limits<int>::max()
        ||  i64 < std::numeric_limits<int>::min()) {
          mbind.buffer_type = MYSQL_TYPE_LONGLONG;
        } else
        if (i64 > std::numeric_limits<short>::max()
        ||  i64 < std::numeric_limits<short>::min()) {
          mbind.buffer_type = MYSQL_TYPE_LONG;
        } else
        if (i64 > std::numeric_limits<char>::max()
        ||  i64 < std::numeric_limits<char>::min()) {
          mbind.buffer_type = MYSQL_TYPE_SHORT;
        } else
          mbind.buffer_type = MYSQL_TYPE_TINY;
        mbind.buffer = const_cast<std::int64_t *>(&i64);
      }
      else if (bind.type() == SQL_FLOAT) {
        double const& d = bind;
        mbind.buffer_type = MYSQL_TYPE_DOUBLE;
        mbind.buffer = const_cast<double *>(&d);
      }
    }
    ::mysql_stmt_bind_param(stmt, mbinds.data());
    return ::mysql_stmt_execute(stmt);
  }

  sqlxx::cursor execute_impl(char const* query, std::vector<sqlxx::field_type> bind) override {
    auto transaction_lock = [&]() {
      auto&& lock = db_();
      transaction tr(lock);
      ::MYSQL_STMT* stmt = ::mysql_stmt_init(lock);
      if (sqlxx::query_has_results(query)) {
        unsigned long attr = CURSOR_TYPE_READ_ONLY,
        rows = std::numeric_limits<unsigned long>::max();
        ::mysql_stmt_attr_set(stmt, STMT_ATTR_CURSOR_TYPE, &attr);
        ::mysql_stmt_attr_set(stmt, STMT_ATTR_PREFETCH_ROWS, &rows);
      }
      if (::mysql_stmt_prepare(stmt, query, strlen(query)) == 0) {
        do_bind(stmt, std::move(bind)) == 0 && tr.commit();
      }
      return stmt;
    };
    return { std::make_shared<statement>(db_, transaction_lock()) };
  }

  db const& db_;
};

class connection : public sqlxx::connection {
public:
  static std::unique_ptr<sqlxx::connection> create(char const* host,
                                                          char const* user,
                                                          char const* pass,
                                                          char const* name) {
    std::unique_ptr<connection> con{ new connection(host, user, pass, name) };
    if (!con->db_.is_open()) con.reset();
    return con;
  }

  void vacuum() override { db_.vacuum(); }
  std::string version() override { return db_.version(); }

  std::unique_ptr<sqlxx::query> query(std::string const& str) override {
    return std::unique_ptr<mysqlxx::query>{ new mysqlxx::query(db_, str) };
  }

private:
  db db_;
  connection(char const* host, char const* user, char const* pass, char const* name)
    : db_{ name ? name : "" } {
      db_.open(host, user, pass);
    }
};

} // namespace mysqlxx

#endif  // _MYSQLXX_H_
