///////////////////////////////////////////////////////////////////////////////
/// \author (c) Marco Paland (marco@paland.com)
///             2014, PALANDesign Hannover, Germany
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

#ifndef _SQLITEXX_H_
#define _SQLITEXX_H_

#include "sqlxx.h"

#include <sqlite3.h>

namespace sqlitexx {

/*
 * Database class
 */
class db
{
public:
  db(std::string const& name, int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE)
    : db_(nullptr)
    , name_(name)
    , open_(false) { open(flags); }

  ~db() { close(); }

  // SQLite3 access
#ifdef USE_SHARED_CONNECTION
  sqlxx::connection_lock<::sqlite3> operator()() const {
    return { mutex_, db_ };
  }
#else
  inline ::sqlite3* operator()() const { return db_; }
#endif

  // open (connect) the database
  bool open(int flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE) {
#ifdef USE_SHARED_CONNECTION
    sqlxx::connection_lock<::sqlite3> lock(mutex_, db_);
#endif
    if (open_) return true;
    int err = ::sqlite3_open_v2(name_.c_str(), &db_, flags, nullptr);
    open_ = err == SQLITE_OK;
    return open_;
  }

  // close the database
  void close() {
#ifdef USE_SHARED_CONNECTION
    sqlxx::connection_lock<::sqlite3> lock(mutex_, db_);
#endif
    if (!open_) return;
    ::sqlite3_close_v2(db_);
    open_ = false;
    db_   = nullptr;
  }

  // returns true if the database is open
  inline bool is_open() const { return open_; }

  // SQLite version
  inline std::string version() { return "SQLITE: " SQLITE_VERSION; }

  // database defragmentation
  int vacuum() { return ::sqlite3_exec((*this)(), "VACUUM;", nullptr, nullptr, nullptr); }

private:
  db(db&&) = delete;            // no move
  db(db const&) = delete;       // no copy
  db& operator=(db&&) = delete; // no assignment
  db& operator=(db const&) = delete; // no assignment

private:
  ::sqlite3*        db_;    // associated db
  std::string const name_;  // db filename
  bool              open_;  // db open status
#ifdef USE_SHARED_CONNECTION
  mutable std::mutex mutex_;
#endif
};

class statement : public sqlxx::statement {
public:
  statement(::sqlite3* db_, ::sqlite3_stmt* stmt) : stmt_(stmt) {
    int result;
    if (!stmt_) {
      result = ::sqlite3_errcode(db_);
    } else {
      result = ::sqlite3_step(stmt_);
    }
    switch(result) {
      case SQLITE_OK:
      case SQLITE_DONE: result_ = SQL_OK; break;
      case SQLITE_NOMEM: result_ = SQL_NO_MEMORY; return;
      case SQLITE_EMPTY: result_ = SQL_IMPROPER; return;
      default: result_ = SQL_UNKNOWN_ERROR; return;
    }
    last_id_ = ::sqlite3_last_insert_rowid(db_);
    affected_rows_ = ::sqlite3_changes(db_);
  }

  statement(statement&&) = delete;
  statement(statement const&) = delete;
  statement& operator=(statement&&) = delete;
  statement& operator=(statement const&) = delete;

  ~statement() override { if (stmt_) ::sqlite3_finalize(stmt_); }

  sqlxx::row next() override {
    sqlxx::row row_;
    if (!stmt_ || ::sqlite3_step(stmt_) != SQLITE_ROW) return {};
    for (int i = 0; i < ::sqlite3_column_count(stmt_); ++i) {
      switch (::sqlite3_column_type(stmt_, i))
      {
      case SQLITE_INTEGER:
        row_.emplace_back(std::int64_t(::sqlite3_column_int64(stmt_, i)), ::sqlite3_column_name(stmt_, i));
        break;
      case SQLITE_FLOAT:
        row_.emplace_back(::sqlite3_column_double(stmt_, i), ::sqlite3_column_name(stmt_, i));
        break;
      case SQLITE_BLOB: {
        auto const* data = reinterpret_cast<std::uint8_t const*>(::sqlite3_column_blob(stmt_, i));
        blob b(data, ::sqlite3_column_bytes(stmt_, i));
        row_.emplace_back(std::move(b), ::sqlite3_column_name(stmt_, i));
      } break;
      case SQLITE_TEXT: {
        std::string text(reinterpret_cast<char const*>(::sqlite3_column_text(stmt_, i)));
        row_.emplace_back(std::move(text), ::sqlite3_column_name(stmt_, i));
      } break;
      case SQLITE_NULL:
        row_.emplace_back(::sqlite3_column_name(stmt_, i));
        break;
      default:
        row_.emplace_back(std::int64_t(0), ::sqlite3_column_name(stmt_, i));
        break;
      }
    }
    return std::move(row_);
  }
  void first() override { if (stmt_) ::sqlite3_reset(stmt_); }
  result_type result() const override { return result_; };
  std::uint64_t last_id() const override { return last_id_; };
  std::uint64_t affected_rows() const override { return affected_rows_; };

private:
  ::sqlite3_stmt* stmt_;
  result_type result_;
  std::uint64_t last_id_ = 0;
  std::uint64_t affected_rows_ = 0;
};

/*
 * Representation of a transaction
 */
class transaction {
public:
  transaction(::sqlite3* db) : db_(db) {
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
    auto err = ::sqlite3_exec(db_, "BEGIN;", nullptr, nullptr, nullptr);
    return err == SQLITE_OK;
  }

  bool commit() {
    if (finished_) return true;
    auto err = ::sqlite3_exec(db_, "COMMIT;", nullptr, nullptr, nullptr);
    finished_ = err == SQLITE_OK;
    return finished_;
  }

  bool rollback() {
    if (finished_) return true;
    auto err = ::sqlite3_exec(db_, "ROLLBACK;", nullptr, nullptr, nullptr);
    finished_ = err == SQLITE_OK;
    return finished_;
  }

private:
  ::sqlite3* db_;
  bool finished_;
};

class query : public sqlxx::query {
public:
  query(db&& db) = delete;
  query(db const& db) : db_(db) {}
  query(db const& db, const std::string &str) : sqlxx::query(str), db_(db) {}

private:
  int do_bind(::sqlite3_stmt* stmt, std::vector<sqlxx::field_type> binds) {
    int err = SQLITE_OK; int idx = 1;
    for (auto it = binds.begin(); it != binds.end(); it = binds.erase(it), idx++) {
      auto const& bind = *it;
      if (bind.type() == SQL_BLOB) {
        auto name = bind.name();
        std::string const& v = bind;
        err = ::sqlite3_bind_blob(stmt, name.empty() ? idx : ::sqlite3_bind_parameter_index(stmt, name.c_str()), v.data(), v.size(), SQLITE_TRANSIENT);
      }
      else if (bind.type() == SQL_TEXT) {
        auto name = bind.name();
        std::string const& s = bind;
        err = ::sqlite3_bind_text(stmt, name.empty() ? idx : ::sqlite3_bind_parameter_index(stmt, name.c_str()), s.c_str(), s.size(), SQLITE_TRANSIENT);
      }
      else if (bind.type() == SQL_NULL) {
        auto name = bind.name();
        err = ::sqlite3_bind_null(stmt, name.empty() ? idx : ::sqlite3_bind_parameter_index(stmt, name.c_str()));
      }
      else if (bind.type() == SQL_INTEGER) {
        auto name = bind.name();
        ::sqlite3_int64 i64 = static_cast<::sqlite3_int64>(std::int64_t(bind));
        err = ::sqlite3_bind_int64(stmt, name.empty() ? idx : ::sqlite3_bind_parameter_index(stmt, name.c_str()), i64);
      }
      else if (bind.type() == SQL_FLOAT) {
        double d = bind;
        auto name = bind.name();
        err = ::sqlite3_bind_double(stmt, name.empty() ? idx : ::sqlite3_bind_parameter_index(stmt, name.c_str()), d);
      }
      if (err != SQLITE_OK) break;
    }
    return err;
  }

  sqlxx::cursor execute_impl(char const* query, std::vector<sqlxx::field_type> bind) override {
    auto&& lock = db_();
    transaction tr(lock);
    ::sqlite3_stmt* stmt = nullptr;
    int err = ::sqlite3_prepare_v2(lock, query, -1, &stmt, nullptr);
    err == SQLITE_OK && (err = do_bind(stmt, std::move(bind)));
    err == SQLITE_OK && tr.commit();
    return { std::make_shared<statement>(lock, stmt) };
  }

  db const& db_;
};

class connection : public sqlxx::connection {
public:
  static std::unique_ptr<sqlxx::connection> create(std::string const& name) {
    return std::unique_ptr<sqlxx::connection>{ new connection(name) };
  }

  void vacuum() override { db_.vacuum(); }
  std::string version() override { return db_.version(); }

  std::unique_ptr<sqlxx::query> query(std::string const& str) override {
    return std::unique_ptr<sqlitexx::query>{ new sqlitexx::query(db_, str) };
  }

private:
  db db_;
  connection(std::string const& name) : db_{ name } {}
};

} // namespace sqlitexx

#endif  // _SQLITEXX_H_
