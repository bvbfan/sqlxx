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

#ifndef _PQSQLXX_H_
#define _PQSQLXX_H_

#include "sqlxx.h"

#include <cctype>
#include <libpq-fe.h>

namespace pqsqlxx {

class pqresult {
public:
  pqresult(pqresult const&) = delete;
  pqresult(::PGresult* res) : res_(res) {}
  pqresult(pqresult&& pq) : res_(pq.res_) { pq.res_ = nullptr; }
  ~pqresult() { if(res_) ::PQclear(res_); }
  pqresult& operator=(pqresult&&) = delete;
  pqresult& operator=(pqresult const&) = delete;
  operator ::PGresult*() { return res_; }
  operator bool() const { return !!res_; }
private:
  ::PGresult* res_;
};

/*
 * Database class
 */
class db {
public:
  db(char const* conninfo) : db_(nullptr), open_(false) { open(conninfo); }
  ~db() { close(); }

  // postgresql access
#ifdef USE_SHARED_CONNECTION
  sqlxx::connection_lock<::PGconn> operator()() const {
    return { mutex_, db_ };
  }
#else
  inline ::PGconn* operator()() const { return db_; }
#endif

  // open (connect) the database
  bool open(char const* conninfo) {
#ifdef USE_SHARED_CONNECTION
    sqlxx::connection_lock<::PGconn> lock(mutex_, db_);
#endif
    if (open_) return true;
    open_ = !!(db_ = ::PQconnectdb(conninfo));
    if (!open_ || ::PQstatus(db_) != CONNECTION_OK) {
      if (db_) ::PQfinish(db_);
      open_ = false;
      db_   = nullptr;
    }
    return open_;
  }

  // close the database
  void close() {
#ifdef USE_SHARED_CONNECTION
    sqlxx::connection_lock<::PGconn> lock(mutex_, db_);
#endif
    if (!open_) return;
    ::PQfinish(db_);
    open_ = false;
    db_   = nullptr;
  }

  // returns true if the database is open
  inline bool is_open() const { return open_; }

  // SQLite version
  inline std::string version() {
    std::stringstream s;
    s << "POSTGRESQL: " << ::PQlibVersion();
    return s.str();
  }

  // database defragmentation
  void vacuum() { pqresult(::PQexec((*this)(), "VACUUM;")); }

private:
  db(db&&) = delete;            // no move
  db(db const&) = delete;       // no copy
  db& operator=(db&&) = delete; // no assignment
  db& operator=(db const&) = delete; // no assignment

private:
  ::PGconn*           db_; // associated db
  bool              open_; // db open status
#ifdef USE_SHARED_CONNECTION
  mutable std::mutex mutex_;
#endif
};

class statement : public sqlxx::statement {
public:
  statement(db const& db, pqresult res, std::string const& cur) : db_(db) {
    result_ = SQL_NO_MEMORY;
    if (!res) return;
    switch(::PQresultStatus(res)) {
      case PGRES_COMMAND_OK:
      case PGRES_NONFATAL_ERROR: result_ = SQL_OK; break;
      case PGRES_BAD_RESPONSE: result_ = SQL_UNKNOWN_ERROR; return;
      case PGRES_EMPTY_QUERY: result_ = SQL_IMPROPER; return;
      case PGRES_FATAL_ERROR: if (::PQstatus(db_()) != CONNECTION_OK) {
        result_ = SQL_SERVER_LOST; return;
      } // fallthrough
      default: result_ = SQL_UNKNOWN_ERROR; return;
    }
    close_ = "CLOSE " + cur;
    fetch_next_ = "FETCH NEXT in " + cur;
    move_first_ = "MOVE BACKWARD ALL in " + cur;
    last_id_ = std::uint64_t(::PQoidValue(res));
    affected_rows_ = std::strtoull(::PQcmdTuples(res), nullptr, 10);
  }

  statement(statement&&) = delete;
  statement(statement const&) = delete;
  statement& operator=(statement&&) = delete;
  statement& operator=(statement const&) = delete;

  ~statement() override {
    if (close_.empty()) return;
    pqresult(::PQexec(db_(), close_.c_str()));
  }

  sqlxx::row next() override {
    if (fetch_next_.empty()) return {};
    pqresult res = ::PQexec(db_(), fetch_next_.c_str());
    if (!res) return {};
    sqlxx::row row;
    if (::PQresultStatus(res) == PGRES_TUPLES_OK && ::PQntuples(res) > 0)
    for (int i = 0; i < ::PQnfields(res); ++i) {
      auto *name = PQfname(res, i);
      if (::PQgetisnull(res, 0, i)) {
        row.emplace_back(name);
        continue;
      }
      // binary format is unsupported
      if (::PQfformat(res, i)) continue;
      auto const* data = ::PQgetvalue(res, 0, i);
      size_t const len = ::PQgetlength(res, 0, i);
      if (!len || !data) {
        row.emplace_back(name);
        continue;
      }
      if (len > 1 && data[0] == '\\' && data[1] == 'x') {
        std::string str;
        for (size_t i = 2; i < len; i += 2) {
          char buf[3] = { data[i], data[i+1] };
          str.push_back(char(std::strtol(buf, nullptr, 16)));
        }
        row.emplace_back(blob(std::move(str)), name);
        continue;
      }
      char *end = nullptr;
      double d = std::strtod(data, &end);
      if (data == end) {
        row.emplace_back(std::string(data, len), name);
        continue;
      }
      if (strchr(data, '.') || strchr(data, ',')) {
        row.emplace_back(d, name);
        continue;
      }
      std::int64_t i64 = std::strtoll(data, nullptr, 10);
      row.emplace_back(i64, name);
    }
    return std::move(row);
  }

  void first() override {
    if(move_first_.empty()) return;
    pqresult(::PQexec(db_(), move_first_.c_str()));
  }

  result_type result() const override { return result_; };
  std::uint64_t last_id() const override { return last_id_; };
  std::uint64_t affected_rows() const override { return affected_rows_; };

private:
  db const& db_;
  std::string close_;
  result_type result_;
  std::string fetch_next_;
  std::string move_first_;
  std::uint64_t last_id_ = 0;
  std::uint64_t affected_rows_ = 0;
};

/*
 * Representation of a transaction
 */
class transaction {
public:
  transaction(::PGconn* db) : db_(db) {
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
    pqresult res = ::PQexec(db_, "BEGIN;");
    return res && ::PQresultStatus(res) == PGRES_COMMAND_OK;
  }

  bool commit() {
    if (finished_) return true;
    pqresult res = ::PQexec(db_, "COMMIT;");
    finished_ = res && ::PQresultStatus(res) == PGRES_COMMAND_OK;
    return finished_;
  }

  bool rollback() {
    if (finished_) return true;
    pqresult res = ::PQexec(db_, "ROLLBACK;");
    finished_ = res && ::PQresultStatus(res) == PGRES_COMMAND_OK;
    return finished_;
  }

private:
  ::PGconn* db_;
  bool finished_;
};

class query : public sqlxx::query {
public:
  query(db&& db) = delete;
  query(db const& db) : db_(db) {}
  query(db const& db, std::string const& str) : sqlxx::query(str), db_(db) {}

private:
  std::string pq_build_query(std::string query, std::string &cursor) {
    using namespace std::regex_constants;
    std::regex blob("\\b(BLOB)\\b", ECMAScript | icase);
    query = std::regex_replace(query, blob, "BYTEA");
    char buf[16] = {};
    std::stringstream r;
    size_t pos = 0, cnt = 0;
    r.rdbuf()->pubsetbuf(buf, 15);
    while ((pos = query.find('?')) != query.npos) {
      r << '$' << ++cnt;
      query.replace(pos, 1, buf);
      r.seekp(0);
    }
    if (sqlxx::query_has_results(query.c_str())) {
#ifdef USE_SHARED_CONNECTION
      std::atomic<size_t> i(0);
#else
      static size_t i = 0;
#endif
      std::stringstream r;
      r << "cursor_" << ++i;
      cursor = r.str();
      query = "DECLARE " + cursor + " SCROLL CURSOR WITH HOLD FOR " + query + ';';
    }
    return query;
  }

  sqlxx::cursor execute_impl(char const* query, std::vector<sqlxx::field_type> binds) override {
    std::string cursor;
    auto q = pq_build_query(query, cursor);
    if (binds.empty()) {
      auto trasaction_lock = [&]() {
        auto&& lock = db_();
        transaction tr(lock);
        auto* res = ::PQexec(lock, q.c_str());
        res && ::PQresultStatus(res) == PGRES_COMMAND_OK && tr.commit();
        return res;
      };
      return { std::make_shared<statement>(db_, trasaction_lock(), cursor) };
    }
    std::vector<int> paramFormats;
    std::vector<int> paramLengths;
    std::vector<std::string> values;
    std::vector<char const*> paramValues;
    for (auto const& bind : binds) {
      switch (bind.type()) {
        case SQL_INTEGER: case SQL_FLOAT:
        case SQL_TEXT: case SQL_BLOB: {
          auto str = bind.toString();
          paramValues.push_back(nullptr);
          paramLengths.push_back(str.size());
          values.push_back(std::move(str));
        } break;
        case SQL_NULL: default: {
          paramValues.push_back(nullptr);
          paramLengths.push_back(0);
        } break;
      }
      paramFormats.push_back(0);
    }
    for (size_t i = 0, v = 0; i < binds.size(); i++) {
      switch (binds[i].type()) {
        case SQL_INTEGER: case SQL_FLOAT:
        case SQL_TEXT: case SQL_BLOB: {
          paramValues[i] = values[v++].data();
        } break;
        default: ;
      }
    }
    auto trasaction_lock = [&]() {
      auto&& lock = db_();
      transaction tr(lock);
      auto* res = ::PQexecParams(lock, q.c_str(), binds.size(), nullptr,
                                paramValues.data(), paramLengths.data(),
                                paramFormats.data(), 0);
      res && ::PQresultStatus(res) == PGRES_COMMAND_OK && tr.commit();
      return res;
    };
    return { std::make_shared<statement>(db_, trasaction_lock(), cursor) };
  }

  db const& db_;
};

class connection : public sqlxx::connection {
public:
  static std::unique_ptr<sqlxx::connection> create(char const* conninfo) {
    std::unique_ptr<connection> con{ new connection(conninfo) };
    if (!con->db_.is_open()) con.reset();
    return std::move(con);
  }

  void vacuum() override { db_.vacuum(); }
  std::string version() override { return db_.version(); }

  std::unique_ptr<sqlxx::query> query(std::string const& str) override {
    return std::unique_ptr<pqsqlxx::query>{ new pqsqlxx::query(db_, str) };
  }

private:
  db db_;
  connection(char const* conninfo) : db_{ conninfo } {}
};

} // namespace pqsqlxx

#endif  // _PQSQLXX_H_
