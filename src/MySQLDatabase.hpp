#include "DatabaseInterface.hpp"
#include "node.hpp"
#include <algorithm>
#include <cstring>
#include <memory>
#include <mysql.h>
#include <sstream>
#include <unordered_map>
#include <vector>

class MySQLDatabase : public DatabaseInterface {
private:
  MYSQL *conn;
  std::shared_ptr<MYSQL_RES> result; // result set of SQL
  query_result prepared_result;
  bool prepared_result_active = false;
  int last_errno = 0;
  std::string last_error;

  struct PreparedStatement {
    MYSQL_STMT *stmt = nullptr;
    std::string template_id;
    unsigned long long execute_count = 0;
  };

  std::unordered_map<std::string, PreparedStatement> prepared_statements;

  // create global static mutex used to initialize connection
  static std::mutex conn_mutex;

  static std::string template_id_for(const std::string &query) {
    std::stringstream ss;
    ss << std::hex << std::hash<std::string>{}(query);
    return ss.str();
  }

  void close_prepared_statements() {
    for (auto &entry : prepared_statements) {
      if (entry.second.stmt != nullptr) {
        mysql_stmt_close(entry.second.stmt);
        entry.second.stmt = nullptr;
      }
    }
    prepared_statements.clear();
  }

  bool set_stmt_error(MYSQL_STMT *stmt) {
    last_errno = mysql_stmt_errno(stmt);
    last_error = mysql_stmt_error(stmt);
    return false;
  }

public:
  MySQLDatabase() {
    conn_mutex.lock();
    conn = mysql_init(NULL);
    conn_mutex.unlock();
    if (conn == nullptr) {
      std::cerr << "Error " << mysql_errno(conn) << ": " << mysql_error(conn)
                << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  bool connect(const workerParams &myParams) override {
    try {
      if (mysql_real_connect(conn, myParams.address.c_str(),
                             myParams.username.c_str(),
                             myParams.password.c_str(), NULL, myParams.port,
                             myParams.socket.c_str(), 0)) {
        return true;
      }
      std::cerr << "Error in mysql connection " << mysql_errno(conn) << ": "
                << mysql_error(conn) << std::endl;
      return false;
    } catch (const std::exception &e) {
      std::cerr << "Exception caught in mysql connection: " << e.what()
                << std::endl;
      return false;
    } catch (...) {
      std::cerr << "Unknown exception caught in mysql connection" << std::endl;
      return false;
    }
  }

  void disconnect() override {
    if (conn) {
      close_prepared_statements();
      mysql_close(conn);
      conn = nullptr;
    }
    mysql_thread_end();
  }

  int get_error_number() override {
    if (last_errno != 0)
      return last_errno;
    return mysql_errno(conn);
  }

  bool execute_query(const std::string &query) override {
    last_errno = 0;
    last_error.clear();
    prepared_result_active = false;
    prepared_result.clear();
    result.reset();
    if (mysql_real_query(conn, query.c_str(), query.size()) != 0) {
      last_errno = mysql_errno(conn);
      last_error = mysql_error(conn);
      return false;
    }

    MYSQL_RES *res = mysql_store_result(conn);
    if (!res) {
      int err = mysql_errno(conn);
      if (err != 0) {
        last_errno = err;
        last_error = mysql_error(conn);
        return false;
      }
    } else {
      result.reset(res, [](MYSQL_RES *r) {
        if (r)
          mysql_free_result(r);
      });
    }
    return true;
  }

  bool execute_prepared_query(const std::string &query,
                              const std::vector<PreparedStatementParam> &params,
                              std::string *template_id,
                              unsigned long long *execute_count,
                              bool *prepared_now) override {
    last_errno = 0;
    last_error.clear();
    prepared_result_active = true;
    prepared_result.clear();
    result.reset();
    if (prepared_now)
      *prepared_now = false;

    auto it = prepared_statements.find(query);
    if (it == prepared_statements.end()) {
      MYSQL_STMT *stmt = mysql_stmt_init(conn);
      if (stmt == nullptr) {
        last_errno = mysql_errno(conn);
        last_error = mysql_error(conn);
        return false;
      }
      if (mysql_stmt_prepare(stmt, query.c_str(), query.size()) != 0) {
        set_stmt_error(stmt);
        mysql_stmt_close(stmt);
        return false;
      }
      PreparedStatement prepared;
      prepared.stmt = stmt;
      prepared.template_id = template_id_for(query);
      auto inserted = prepared_statements.emplace(query, prepared);
      it = inserted.first;
      if (prepared_now)
        *prepared_now = true;
    }

    PreparedStatement &prepared = it->second;
    MYSQL_STMT *stmt = prepared.stmt;
    if (template_id)
      *template_id = prepared.template_id;

    if (mysql_stmt_param_count(stmt) != params.size()) {
      last_errno = 2031; // CR_PARAMS_NOT_BOUND
      last_error = "prepared statement parameter count mismatch";
      return false;
    }

    std::vector<MYSQL_BIND> bind(params.size());
    std::vector<unsigned long> lengths(params.size());
    std::unique_ptr<bool[]> is_null(new bool[params.size()]);
    for (size_t i = 0; i < params.size(); i++) {
      std::memset(&bind[i], 0, sizeof(MYSQL_BIND));
      is_null[i] = params[i].is_null;
      if (params[i].is_null) {
        bind[i].buffer_type = MYSQL_TYPE_NULL;
      } else {
        bind[i].buffer_type = MYSQL_TYPE_STRING;
        bind[i].buffer = const_cast<char *>(params[i].value.c_str());
        lengths[i] = params[i].value.size();
        bind[i].length = &lengths[i];
      }
      bind[i].is_null = &is_null[i];
    }

    if (!bind.empty() && mysql_stmt_bind_param(stmt, bind.data()) != 0)
      return set_stmt_error(stmt);
    if (mysql_stmt_execute(stmt) != 0)
      return set_stmt_error(stmt);
    if (mysql_stmt_store_result(stmt) != 0)
      return set_stmt_error(stmt);

    MYSQL_RES *metadata = mysql_stmt_result_metadata(stmt);
    if (metadata != nullptr) {
      unsigned int field_count = mysql_num_fields(metadata);
      const unsigned long buffer_size = 8192;
      std::vector<MYSQL_BIND> result_bind(field_count);
      std::vector<std::vector<char>> buffers(field_count,
                                             std::vector<char>(buffer_size));
      std::vector<unsigned long> result_lengths(field_count);
      std::unique_ptr<bool[]> result_is_null(new bool[field_count]);
      for (unsigned int i = 0; i < field_count; i++) {
        std::memset(&result_bind[i], 0, sizeof(MYSQL_BIND));
        result_bind[i].buffer_type = MYSQL_TYPE_STRING;
        result_bind[i].buffer = buffers[i].data();
        result_bind[i].buffer_length = buffer_size;
        result_bind[i].length = &result_lengths[i];
        result_bind[i].is_null = &result_is_null[i];
      }
      if (field_count > 0 &&
          mysql_stmt_bind_result(stmt, result_bind.data()) != 0) {
        mysql_free_result(metadata);
        return set_stmt_error(stmt);
      }
      int fetch_status;
      while ((fetch_status = mysql_stmt_fetch(stmt)) == 0 ||
             fetch_status == MYSQL_DATA_TRUNCATED) {
        std::vector<std::string> row;
        row.reserve(field_count);
        for (unsigned int i = 0; i < field_count; i++) {
          if (result_is_null[i]) {
            row.emplace_back();
          } else {
            auto length = std::min(result_lengths[i], buffer_size);
            row.emplace_back(buffers[i].data(), length);
          }
        }
        prepared_result.push_back(std::move(row));
      }
      if (fetch_status != MYSQL_NO_DATA) {
        mysql_free_result(metadata);
        return set_stmt_error(stmt);
      }
      mysql_free_result(metadata);
    }

    mysql_stmt_free_result(stmt);
    prepared.execute_count++;
    if (execute_count)
      *execute_count = prepared.execute_count;
    return true;
  }

  int get_affected_rows() override {
    if (prepared_result_active)
      return prepared_result.size();
    if (result == nullptr) {
      return mysql_affected_rows(conn);
    }
    return mysql_num_rows(result.get());
  }

  /* get get the query result from last executed query */
  query_result get_result() override {
    query_result result_set;
    if (prepared_result_active) {
      return prepared_result;
    }
    if (result == nullptr) {
      assert(0);
      exit(EXIT_FAILURE);
    }
    auto total_fields = mysql_num_fields(result.get());
    while (auto row = mysql_fetch_row(result.get())) {
      std::vector<std::string> r;
      for (unsigned int i = 0; i < total_fields; i++) {
        std::string value;
        if (row[i] != NULL)
          value = row[i];
        r.push_back(value);
      }
      result_set.push_back(r);
    }
    return result_set;
  }

  query_result get_query_result(const std::string &query) override {
    query_result result_set;
    if (execute_query(query)) {
      if (result == nullptr) {
        return result_set;
      }
      auto total_fields = mysql_num_fields(result.get());
      while (auto row = mysql_fetch_row(result.get())) {
        std::vector<std::string> r;
        for (unsigned int i = 0; i < total_fields; i++) {
          std::string value;
          if (row[i] != NULL)
            value = row[i];
          r.push_back(value);
        }
        result_set.push_back(r);
      }
    }
    return result_set;
  }

  std::string get_single_value(const std::string &query) override {
    execute_query(query);
    if (result == nullptr) {
      return "";
    }
    auto row = mysql_fetch_row(result.get());
    if (row) {
      return row[0];
    }
    return "";
  }

  std::string get_error() override {
    if (!last_error.empty())
      return last_error;
    return mysql_error(conn);
  }

  int get_server_version() override {
    static const int server_version = []() {
      std::string version_str = mysql_get_client_info();
      size_t pos = version_str.find_first_of("0123456789"); // Find first digit
      if (pos == std::string::npos)
        return 0; // No version number found

      int major = 0, minor = 0, patch = 0;
      sscanf(version_str.c_str() + pos, "%d.%d.%d", &major, &minor, &patch);
      return major * 10000 + minor * 100 + patch;
    }();
    return server_version;
  }

  ~MySQLDatabase() { disconnect(); }
};
