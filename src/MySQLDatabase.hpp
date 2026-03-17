#include "DatabaseInterface.hpp"
#include "node.hpp"
#include <memory>
#include <mysql.h>

class MySQLDatabase : public DatabaseInterface {
private:
  MYSQL *conn;
  std::shared_ptr<MYSQL_RES> result; // result set of SQL
  // create global static mutex used to initialize connection
  static std::mutex conn_mutex;

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
      mysql_close(conn);
    }
    mysql_thread_end();
  }

  int get_error_number() override { return mysql_errno(conn); }

  bool execute_query(const std::string &query) override {
    if (mysql_real_query(conn, query.c_str(), query.size()) != 0) {
      return false;
    }

    MYSQL_RES *res = mysql_store_result(conn);
    if (!res) {
      int err = mysql_errno(conn);
      if (err != 0) {
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

  int get_affected_rows() override {
    if (result == nullptr) {
      return mysql_affected_rows(conn);
    }
    return mysql_num_rows(result.get());
  }

  /* get get the query result from last executed query */
  query_result get_result() override {
    query_result result_set;
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

  std::string get_error() override { return mysql_error(conn); }

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
