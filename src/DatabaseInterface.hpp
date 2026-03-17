#pragma once
#include <string>
#include <vector>
struct workerParams;
typedef std::vector<std::vector<std::string>> query_result;

class DatabaseInterface {
public:
  virtual bool connect(const workerParams &wparam) = 0;
  virtual void disconnect() = 0;
  virtual bool execute_query(const std::string &query) = 0;
  virtual query_result get_query_result(const std::string &query) = 0;
  virtual query_result get_result() = 0;
  virtual std::string get_single_value(const std::string &query) = 0;
  virtual int get_affected_rows() = 0;
  virtual std::string get_error() = 0;
  virtual int get_error_number() = 0;
  virtual ~DatabaseInterface() = default;
  virtual int get_server_version() = 0;
};

/*
#ifdef USE_MYSQL
#include "MySQLDatabase.hpp"
#elif defined(USE_DUCKDB)
#include "DuckDBDatabase.hpp"
#endif
*/
