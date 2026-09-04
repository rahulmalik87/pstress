#pragma once
#include <string>
#include <vector>
struct workerParams;
typedef std::vector<std::vector<std::string>> query_result;

/* Values get_error_number() reports. They are MySQL client error codes because
   the workload's error handling in random_test.cpp was written against them:
   CR_SERVER_LOST is what makes it log the query as fatal and fail the run's
   exit code. ClickHouse has no equivalent codes of its own, so
   ClickHouseDatabase maps every transport-level failure onto CR_SERVER_LOST. */
#define CR_WSREP_NOT_PREPARED 1047
#define CR_SERVER_GONE_ERROR 2006
#define CR_SERVER_LOST 2013
#define CR_SECONDARY_NOT_READY 6000

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
