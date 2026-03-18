#pragma once
#include "DatabaseInterface.hpp"
#include "node.hpp"
#include <clickhouse/client.h>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

static std::string ch_col_to_string(const clickhouse::ColumnRef &col,
                                    size_t row) {
  using namespace clickhouse;
  switch (col->Type()->GetCode()) {
  case Type::String:
  case Type::FixedString:
    return std::string(col->As<ColumnString>()->At(row));
  case Type::UInt8:
    return std::to_string(col->As<ColumnUInt8>()->At(row));
  case Type::UInt16:
    return std::to_string(col->As<ColumnUInt16>()->At(row));
  case Type::UInt32:
    return std::to_string(col->As<ColumnUInt32>()->At(row));
  case Type::UInt64:
    return std::to_string(col->As<ColumnUInt64>()->At(row));
  case Type::Int8:
    return std::to_string(col->As<ColumnInt8>()->At(row));
  case Type::Int16:
    return std::to_string(col->As<ColumnInt16>()->At(row));
  case Type::Int32:
    return std::to_string(col->As<ColumnInt32>()->At(row));
  case Type::Int64:
    return std::to_string(col->As<ColumnInt64>()->At(row));
  case Type::Float32:
    return std::to_string(col->As<ColumnFloat32>()->At(row));
  case Type::Float64:
    return std::to_string(col->As<ColumnFloat64>()->At(row));
  case Type::Date:
    return std::to_string(col->As<ColumnDate>()->At(row).count());
  case Type::DateTime:
    return std::to_string(col->As<ColumnDateTime>()->At(row));
  case Type::Nullable: {
    auto nullable = col->As<ColumnNullable>();
    if (nullable->IsNull(row))
      return "";
    return ch_col_to_string(nullable->Nested(), row);
  }
  default:
    return "";
  }
}

class ClickHouseDatabase : public DatabaseInterface {
private:
  std::unique_ptr<clickhouse::Client> client;
  query_result last_result;
  std::string last_error;
  int last_error_number = 0;

public:
  ClickHouseDatabase() = default;

  bool connect(const workerParams &myParams) override {
    try {
      clickhouse::ClientOptions opts;
      opts.SetHost(myParams.address)
          .SetPort(myParams.port > 0 ? myParams.port : 9000)
          .SetUser(myParams.username)
          .SetPassword(myParams.password)
          .SetDefaultDatabase(myParams.database);
      client = std::make_unique<clickhouse::Client>(opts);
      return true;
    } catch (const std::exception &e) {
      std::cerr << "ClickHouse connect error: " << e.what() << std::endl;
      return false;
    }
  }

  void disconnect() override { client.reset(); }

  bool execute_query(const std::string &query) override {
    last_result.clear();
    last_error.clear();
    last_error_number = 0;
    try {
      client->Execute(
          clickhouse::Query(query).OnData([&](const clickhouse::Block &block) {
            for (size_t row = 0; row < block.GetRowCount(); ++row) {
              std::vector<std::string> row_data;
              for (size_t col = 0; col < block.GetColumnCount(); ++col) {
                row_data.push_back(ch_col_to_string(block[col], row));
              }
              last_result.push_back(row_data);
            }
          }));
      return true;
    } catch (const std::exception &e) {
      last_error = e.what();
      last_error_number = 1;
      return false;
    }
  }

  query_result get_result() override { return last_result; }

  query_result get_query_result(const std::string &query) override {
    execute_query(query);
    return last_result;
  }

  std::string get_single_value(const std::string &query) override {
    if (!execute_query(query))
      return "";
    if (!last_result.empty() && !last_result[0].empty())
      return last_result[0][0];
    return "";
  }

  int get_affected_rows() override {
    return static_cast<int>(last_result.size());
  }

  std::string get_error() override { return last_error; }

  int get_error_number() override { return last_error_number; }

  int get_server_version() override {
    std::string ver = get_single_value("SELECT version()");
    if (ver.empty())
      return 0;
    int major = 0, minor = 0, patch = 0;
    sscanf(ver.c_str(), "%d.%d.%d", &major, &minor, &patch);
    return major * 10000 + minor * 100 + patch;
  }

  ~ClickHouseDatabase() = default;
};
