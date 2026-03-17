#include "DatabaseInterface.hpp"
#include "node.hpp"
#include <duckdb.h> // Include the C API header
#include <duckdb.hpp>
#include <iostream>
#include <memory>
#include <vector>

class DuckDBDatabase : public DatabaseInterface {
private:
  std::unique_ptr<duckdb::Connection> conn;
  std::unique_ptr<duckdb::QueryResult> result;

  /* return the shared database instance */
  static std::shared_ptr<duckdb::DuckDB> &
  getSharedDB(const std::string &logdir) {
    const std::string file_path = logdir + "/duckdb";
    std::cout << "Using DuckDB database at " << file_path << std::endl;
    static std::shared_ptr<duckdb::DuckDB> shared_db =
        std::make_shared<duckdb::DuckDB>(file_path);
    return shared_db;
  }

public:
  DuckDBDatabase() {
    // Initialize the shared database only once
  }


  int get_server_version() override {
    const char *version_str = duckdb_library_version();
    if (!version_str) {
      std::cerr << "Failed to retrieve DuckDB version via C API" << std::endl;
      return 0; // Fallback if version is unavailable
    }

    int major = 0, minor = 0, patch = 0;
    if (version_str[0] == 'v') {
      sscanf(version_str, "v%d.%d.%d", &major, &minor, &patch);
    } else {
      sscanf(version_str, "%d.%d.%d", &major, &minor, &patch);
    }
    return major * 10000 + minor * 100 + patch; // e.g., 1.1.4 -> 10104
  }

  bool connect(const workerParams &myParams) override {
    conn = std::make_unique<duckdb::Connection>(*getSharedDB(myParams.logdir));
    return true;
  }

  void disconnect() override {}

  int get_error_number() override {
    return result && result->HasError() ? 1 : 0;
    }

    bool execute_query(const std::string &query) override {
      result = conn->Query(query);
      return !result->HasError();
    }

    int get_affected_rows() override {
      if (result &&
          result->type == duckdb::QueryResultType::MATERIALIZED_RESULT) {
        auto materialized =
            static_cast<duckdb::MaterializedQueryResult *>(result.get());
        return materialized->RowCount();
      }
      return 0;
    }
    query_result get_result() override {
      query_result result_set;
      if (result->type == duckdb::QueryResultType::MATERIALIZED_RESULT) {
        auto materialized =
            static_cast<duckdb::MaterializedQueryResult *>(result.get());
        for (size_t row_idx = 0; row_idx < materialized->RowCount();
             ++row_idx) {
          std::vector<std::string> row_data;
          for (size_t col_idx = 0; col_idx < materialized->names.size();
               ++col_idx) {
            row_data.push_back(
                materialized->GetValue(col_idx, row_idx).ToString());
          }
          result_set.push_back(row_data);
        }
      }
      return result_set;
    }

    query_result get_query_result(const std::string &query) override {
      query_result result_set;
      if (!execute_query(query)) {
        return result_set;
      }
      if (result->type == duckdb::QueryResultType::MATERIALIZED_RESULT) {
        auto materialized =
            static_cast<duckdb::MaterializedQueryResult *>(result.get());
        for (size_t row_idx = 0; row_idx < materialized->RowCount();
             ++row_idx) {
          std::vector<std::string> row_data;
          for (size_t col_idx = 0; col_idx < materialized->names.size();
               ++col_idx) {
            row_data.push_back(
                materialized->GetValue(col_idx, row_idx).ToString());
          }
          result_set.push_back(row_data);
        }
      }
      return result_set;
    }

    std::string get_single_value(const std::string &query) override {
      if (!execute_query(query)) {
        return "";
      }
      if (result->type == duckdb::QueryResultType::MATERIALIZED_RESULT) {
        auto materialized =
            static_cast<duckdb::MaterializedQueryResult *>(result.get());
        if (materialized->RowCount() > 0) {
          return materialized->GetValue(0, 0).ToString();
        }
      }
      return "";
    }

    std::string get_error() override {
      return result && result->HasError() ? result->GetError() : "";
    }

    ~DuckDBDatabase() = default;
};
