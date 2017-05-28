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

#ifndef _SQL_XX_H_
#define _SQL_XX_H_

#include <tuple>
#include <cmath>
#include <regex>
#include <limits>
#include <memory>
#include <vector>
#include <string>
#include <utility>
#include <sstream>
#include <cstdint>
#include <cstring>
#include <iomanip>
#include <iterator>
#include <algorithm>
#include <initializer_list>

#ifdef USE_SHARED_CONNECTION
#include <mutex>
#include <atomic>
#endif

typedef std::initializer_list<std::string> format;

template<class...Args> inline
auto values(Args&&... p) -> decltype(std::forward_as_tuple(std::forward<Args>(p)...)) {
  return std::forward_as_tuple(std::forward<Args>(p)...);
}

template<class T, class N> inline
auto value(T&& t, N&& n) -> decltype(std::make_pair(std::forward<T>(t), std::forward<N>(n))) {
  return std::make_pair(std::forward<T>(t), std::forward<N>(n));
}

enum sql_type {
  SQL_INVALID = -1,
  SQL_NULL,
  SQL_INTEGER,
  SQL_FLOAT,
  SQL_TEXT,
  SQL_BLOB
};

enum result_type {
  SQL_OK,
  SQL_IMPROPER,
  SQL_NO_MEMORY,
  SQL_SERVER_LOST,
  SQL_UNKNOWN_ERROR,
};

class blob {
public:
  typedef std::uint8_t value_type;
  typedef value_type& reference;
  typedef value_type const& const_reference;
  typedef std::basic_string<value_type>::iterator iterator;
  typedef std::basic_string<value_type>::const_iterator const_iterator;

  blob(blob const&) = delete;
  blob(blob&& b) : data_(std::move(b.data_)) {}
  blob(std::string&& s) : data_(std::move(s)) {}
  blob(size_t size, value_type value = 0) : data_(size, char(value)) {}
  blob(value_type const* p, size_t size) : data_(reinterpret_cast<char const*>(p), size) {}
  value_type const* data() const { return reinterpret_cast<value_type const*>(data_.data()); }

  iterator begin() { return iterator(reinterpret_cast<value_type*>(&(data_[0]))); }
  iterator end() { return iterator(reinterpret_cast<value_type*>(&(data_[size()]))); }
  const_iterator cbegin() { return begin(); }
  const_iterator cend() { return end(); }
  const_iterator begin() const { return const_iterator(reinterpret_cast<value_type const*>(&(data_[0]))); }
  const_iterator end() const { return const_iterator(reinterpret_cast<value_type const*>(&(data_[size()]))); }

  inline size_t size() const { return data_.size(); }
  inline bool empty() const { return data_.empty(); }
  inline void clear() { data_.clear(); }

  operator std::string&&() && { return std::move(data_); }
  operator std::string const&() const { return data_; }

  bool operator==(blob const& o) const { return data_ == o.data_; }
  bool operator!=(blob const& o) const { return data_ != o.data_; }

  reference operator[](size_t pos) { return reinterpret_cast<reference>(data_[pos]); }
  const_reference operator[](size_t pos) const { return reinterpret_cast<const_reference>(data_[pos]); }

private:
  std::string data_;
};

namespace sqlxx {

#ifdef USE_SHARED_CONNECTION
template<class T>
class connection_lock {
public:
  connection_lock(connection_lock&&) = delete;
  connection_lock(connection_lock const&) = delete;
  connection_lock(std::mutex& mutex, T* value)
                 : value_(value), mutex_(mutex) { mutex_.lock(); }
  ~connection_lock() { mutex_.unlock(); }
  operator T*() { return value_; }
  connection_lock& operator=(connection_lock&&) = delete;
  connection_lock& operator=(connection_lock const&) = delete;
private:
  T* value_;
  std::mutex& mutex_;
};
#endif

/*
 * Test query produce results
 */
static
bool query_has_results(char const* query) {
  using namespace std::regex_constants;
  std::regex const expect_results[] = {
    std::regex("\\b(DESC)\\b", ECMAScript | icase),
    std::regex("\\b(SHOW)\\b", ECMAScript | icase),
    std::regex("\\b(EXPLAIN)\\b", ECMAScript | icase),
    std::regex("\\b(DESCRIBE)\\b", ECMAScript | icase),
    std::regex("\\b(SELECT)\\b(?![^\\(]*\\))", ECMAScript | icase),
  };
  for (auto const& expect : expect_results) {
    if (std::regex_search(query, expect)) {
      return true;
    }
  }
  return false;
}

/*
 * Representation of a single result field
 */
struct field_type {
  // ctors
  field_type() {}
  field_type(field_type const& other) { operator=(other); }
  field_type(field_type&& other) { operator=(std::move(other)); }
  field_type(std::string const& name) : name_(name), type_(SQL_NULL) {}
  field_type(std::int64_t i, std::string const& name)
    : name_(name), type_(SQL_INTEGER) { int_ = i;  float_ = double(int_); }
  field_type(double d, std::string const& name)
    : name_(name), type_(SQL_FLOAT)  { float_ = d; int_ = std::int64_t(d); }
  field_type(std::string&& s, std::string const& name)
    : name_(name), type_(SQL_TEXT) { str_ = std::move(s); }
  field_type(std::string const& s, std::string const& name)
    : name_(name), type_(SQL_TEXT) { str_ = s; }
  explicit field_type(blob&& b, std::string const& name)
    : name_(name), type_(SQL_BLOB) { str_ = std::move(b); }
  explicit field_type(blob const& b, std::string const& name)
    : name_(name), type_(SQL_BLOB) { str_ = b; }

  field_type& operator=(field_type const& other) {
    if (this != &other) {
      str_ = other.str_;
      int_ = other.int_;
      name_ = other.name_;
      type_ = other.type_;
      float_ = other.float_;
    }
    return *this;
  }

  field_type& operator=(field_type&& other) {
    if (this != &other) {
      str_ = std::move(other.str_);
      int_ = std::move(other.int_);
      name_ = std::move(other.name_);
      type_ = std::move(other.type_);
      float_ = std::move(other.float_);
    }
    return *this;
  }

  // access field value
  operator int() const { return static_cast<int>(int_); }
  operator char() const { return static_cast<char>(int_); }
  operator short() const { return static_cast<short>(int_); }
  operator float() const { return static_cast<float>(float_); }
  operator double const&() const { return float_; }
  operator std::string const&() const { return str_; }
  operator std::int64_t const&() const { return int_; }

  bool operator==(std::string const& str) const { return type_ == SQL_TEXT && str == str_; }
  bool operator==(std::int64_t i64) const { return type_ == SQL_INTEGER && i64 == int_; }
  bool operator==(short i16) const { return type_ == SQL_INTEGER && i16 == short(int_); }
  bool operator==(int i32) const { return type_ == SQL_INTEGER && i32 == int(int_); }
  bool operator==(char i8) const { return type_ == SQL_INTEGER && i8 == char(int_); }
  bool operator==(float f) const { return type_ == SQL_FLOAT && f == float(float_); }
  bool operator==(double d) const { return type_ == SQL_FLOAT && d == float_; }

  std::string toString() const {
    switch (type_) {
      case SQL_TEXT    : return str_;
      case SQL_INTEGER : { std::stringstream s; s << int_; return s.str(); }
      case SQL_FLOAT   : { std::stringstream s; s << float_; return s.str(); }
      case SQL_BLOB    : { std::stringstream s; s << "\\x" << std::hex << std::setfill('0');
                             for (auto const& v : str_) s << (int(v)&0xFF); return s.str(); }
      case SQL_NULL    : return "NULL";
      default          : return "INVALID";
    }
  }

  // column name
  inline std::string name() const { return name_; }

  // column type
  inline sql_type type() const { return type_; }

  // returns true if field is NULL
  inline bool is_null() const { return type_ == SQL_NULL; }

  bool operator==(field_type const& f) const {
    return type_ == f.type_ && int_ == f.int_
        && str_ == f.str_ && name_ == f.name_
        && std::fabs(float_ - f.float_) < std::numeric_limits<double>::epsilon();
  }

private:
  std::int64_t          int_ = {};   // int data
  double                float_ = {}; // float data
  std::string           str_;        // string (blob) data
  std::string           name_;       // field (col) name
  sql_type              type_ = SQL_INVALID; // sqlte type
};

/*
 * Invalid reference
 */
template<class T> static
T const& invalid() {
  static const T invalid_ref;
  return invalid_ref;
}

/*
 * Representation of a result row
 */
class row : public std::vector<field_type> {
public:
  // access field by index
  const_reference operator[](size_type idx) const {
    return idx < size() ? std::vector<field_type>::operator[](idx) : invalid<field_type>();
  }

  // access field by name
  const_reference operator[](char const* colname) const {
    for (row::const_iterator it = begin(); it != end(); ++it) {
      if (colname && it->name() == colname) {
        return *it;
      }
    }
    return invalid<field_type>();  // no match - invalid answer
  }

  bool operator==(row const& r) const {
    return size() == r.size() && std::equal(begin(), end(), r.begin());
  }
};

/*
 * Representation of a complete result set (all columns and rows)
 */
class result : public std::vector<row> {
public:
  const_reference operator[](size_type idx) const {
    return idx < size() ? std::vector<row>::operator[](idx) : invalid<row>();
  }
  result& operator+=(row const& row) {
    push_back(row);
    return *this;
  }
  result& operator+=(row&& row) {
    push_back(std::move(row));
    return *this;
  }
};

class statement {
public:
  virtual ~statement() {};
  virtual row next() = 0;
  virtual void first() = 0;
  virtual result_type result() const = 0;
  virtual std::uint64_t last_id() const = 0;
  virtual std::uint64_t affected_rows() const = 0;
};

class iterator {
public:
  using value_type = row;
  using pointer = value_type*;
  using reference = value_type&;
  using difference_type = std::ptrdiff_t;
  using iterator_category = std::forward_iterator_tag;

  iterator(std::weak_ptr<statement> stmt) : stmt_(stmt) {
    next();
  }

  iterator(iterator&&) = default;
  iterator(iterator const&) = default;
  iterator& operator=(iterator&&) = default;
  iterator& operator=(iterator const&) = default;

  row& operator*() { return row_; }
  row* operator->() { return &row_; }
  iterator& operator++() { next(); return *this; }

  iterator operator++(int) {
    auto row = row_;
    operator++();
    return { std::move(row), stmt_ };
  }

  bool operator==(iterator const& q) const {
    return row_ == q.row_;
  }

  bool operator!=(iterator const& q) const {
    return !operator==(q);
  }

private:
  void next() {
    if (auto stmt = stmt_.lock())
      row_ = stmt->next();
  }

  row row_;
  std::weak_ptr<statement> stmt_;
  iterator(row&& row, std::weak_ptr<statement> stmt)
    : row_(std::move(row))
    , stmt_(stmt) {}
};

class cursor {
public:
  cursor(std::shared_ptr<statement> stmt) : stmt_(stmt) {}
  cursor(cursor&&) = default;
  cursor(cursor const&) = delete;
  cursor& operator=(cursor&&) = delete;
  cursor& operator=(cursor const&) = delete;

  iterator begin() { stmt_->first(); return { stmt_ }; }
  iterator end() { return { std::shared_ptr<statement>() }; }

  result_type result() const { return stmt_->result(); }
  std::uint64_t last_id() const { return stmt_->last_id(); }
  std::uint64_t affected_rows() const { return stmt_->affected_rows(); }

private:
  std::shared_ptr<statement> stmt_;
};

/*
 * Representation of a query
 */
class query
{
public:
  query(std::string const& str = {}) {
    (*this) << str.c_str(); // escape
  }

  virtual ~query() {}

  template <class T>
  query& operator<< (T&& t) {
    return bind({}, std::forward<T>(t));
  }

  template<class T, size_t N, size_t M>
  struct expand {
    static void tuple(query& q, T&& t) {
      q.bind(std::get<N>(std::forward<T>(t)));
      expand<T, N+1, M>::tuple(q, std::forward<T>(t));
    }
  };

  template<class T, size_t N>
  struct expand<T, N, N> {
    static void tuple(query&, T&&) {}
  };

  template<class... Args>
  query& operator<<(std::tuple<Args...> t) {
    expand<decltype(t), 0, sizeof...(Args)>::tuple(*this, std::forward<std::tuple<Args...>>(t));
    return *this;
  }

  template<class T1, class T2>
  struct binder {
    static void pair(query&, std::pair<T1, T2>&&) {
      static_assert(std::is_same<T1, char const*>::value
                 || std::is_same<T1, std::string>::value, "name should be a string");
    }
  };

  template<size_t N, class T2>
  struct binder<char const (&)[N], T2> {
    static void pair(query& q, std::pair<char const (&)[N], T2>&& p) {
      q.bind(p.first, std::forward<T2>(p.second));
    }
  };

  template<class T2>
  struct binder<char const*, T2> {
    static void pair(query& q, std::pair<char const*, T2>&& p) {
      q.bind(p.first, std::forward<T2>(p.second));
    }
  };

  template<class T2>
  struct binder<std::string, T2> {
    static void pair(query& q, std::pair<std::string, T2>&& p) {
      q.bind(p.first, std::forward<T2>(p.second));
    }
  };

  template<class T1, class T2>
  query& operator<<(std::pair<T1, T2> p) {
    binder<T1, T2>::pair(*this, std::forward<std::pair<T1, T2>>(p));
    return *this;
  }

  query& operator<< (format str) {
    char buf[16] = {};
    std::stringstream r;
    size_t pos = 0, i = 0;
    auto q = query_.str(); query_.str({});
    r.rdbuf()->pubsetbuf(buf, 15);
    for (auto const&s : str) {
      r << '{' << i++ << '}';
      while ((pos = q.find(buf)) != q.npos)
        q.replace(pos, strlen(buf), s);
      r.seekp(0);
    }
    query_ << std::move(q);
    return *this;
  }

  query& operator<< (char const* text)
  {
    if (!text || !*text) {
      return *this;
    }
    if (!strpbrk(text, "'\\")) {
      query_ << text;
      return *this;
    }
    for (size_t i = 0; text[i]; ++i) {
      query_ << text[i];
      if (text[i] == '\'') query_ << '\'';
      if (text[i] == '\\') query_ << '\\';
    }
    return *this;
  }

  cursor execute() {
    auto cursor = execute_impl(query_.str().c_str(), std::move(bind_));
    query_.str({});
    return cursor;
  }

  // bind to query
  template<class T>
  query& bind(T&& t) {
    return bind({}, std::forward<T>(t));
  }

  query& bind(std::string const& param, float f) {
    bind_.emplace_back(double(f), param);
    return *this;
  }

  query& bind(std::string const& param, char c) {
    bind_.emplace_back(std::int64_t(c), param);
    return *this;
  }

  query& bind(std::string const& param, short s) {
    bind_.emplace_back(std::int64_t(s), param);
    return *this;
  }

  query& bind(std::string const& param, int i) {
    bind_.emplace_back(std::int64_t(i), param);
    return *this;
  }

  query& bind(std::string const& param, size_t s) {
    bind_.emplace_back(std::int64_t(s), param);
    return *this;
  }

  template<class T>
  query& bind(std::string const& param, T&& t) {
    bind_.emplace_back(std::forward<T>(t), param);
    return *this;
  }

protected:
  // bind function
  virtual cursor execute_impl(char const* query, std::vector<field_type> bind) = 0;

private:
  std::stringstream query_;
  std::vector<field_type> bind_;
};

class connection {
public:
  virtual ~connection() {}
  virtual void vacuum() = 0;
  virtual std::string version() = 0;
  virtual std::unique_ptr<sqlxx::query> query(std::string const& str = {}) = 0;
};

} // namespace sqlxx

#endif  // _SQL_XX_H_
