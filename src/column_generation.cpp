#include "random_test.hpp"
#include <document.h>
extern std::mutex all_table_mutex;
extern std::vector<Table *> *all_tables;
extern int number_of_records;
extern std::atomic<bool> run_query_failed;
using namespace rapidjson;

std::string Column::rand_value() {
  switch (type_) {
  case INT:
  case INTEGER:
    return std::to_string(rand_int(1000));
  case FLOAT:
    return rand_float(1000.0);
  case DOUBLE:
    return rand_double(1000.0);
  case BOOL:
    return rand_int(2) == 0 ? "TRUE" : "FALSE";
  case DATE:
    return "CURDATE()";
  case DATETIME:
    return "NOW()";
  case TIMESTAMP:
    return "CURRENT_TIMESTAMP";
  case BIT:
    return "b'" + std::to_string(rand_int(2)) + "'";
  case BLOB:
    return "NULL";
  case CHAR:
  case VARCHAR:
    return "'" + rand_string(10) + "'";
  case TEXT:
    return "'" + rand_string(100) + "'";
  case GENERATED:
    return "DEFAULT";
  case JSON:
    return "'" + json_rand_doc(this) + "'";
  default:
    return "NULL";
  }
}

std::string Column::rand_value_universal() {
  switch (type_) {
  case INT:
  case INTEGER:
    return std::to_string(rand_int(1000));
  case FLOAT:
    return rand_float(1000.0);
  case DOUBLE:
    return rand_double(1000.0);
  case BOOL:
    return rand_int(2) == 0 ? "TRUE" : "FALSE";
  case DATE:
    return "CURDATE()";
  case DATETIME:
    return "NOW()";
  case TIMESTAMP:
    return "CURRENT_TIMESTAMP";
  case BIT:
    return "b'" + std::to_string(rand_int(2)) + "'";
  case BLOB:
    return "NULL";
  case CHAR:
  case VARCHAR:
    return "'" + rand_string(10) + "'";
  case TEXT:
    return "'" + rand_string(100) + "'";
  case GENERATED:
    return "DEFAULT";
  case JSON:
    return "'" + json_rand_doc(this) + "'";
  default:
    return "NULL";
  }
}

const std::string Column::col_type_to_string(COLUMN_TYPES type) {
  switch (type) {
  case INT:
    return "INT";
  case INTEGER:
    return "INTEGER";
  case FLOAT:
    return "FLOAT";
  case DOUBLE:
    return "DOUBLE";
  case BOOL:
    return "BOOL";
  case DATE:
    return "DATE";
  case DATETIME:
    return "DATETIME";
  case TIMESTAMP:
    return "TIMESTAMP";
  case BIT:
    return "BIT";
  case BLOB:
    return "BLOB";
  case CHAR:
    return "CHAR";
  case VARCHAR:
    return "VARCHAR";
  case TEXT:
    return "TEXT";
  case GENERATED:
    return "GENERATED";
  case JSON:
    return "JSON";
  default:
    return "UNKNOWN";
  }
}

Column::COLUMN_TYPES Column::col_type(std::string type) {
  if (type == "INT")
    return INT;
  if (type == "INTEGER")
    return INTEGER;
  if (type == "FLOAT")
    return FLOAT;
  if (type == "DOUBLE")
    return DOUBLE;
  if (type == "BOOL")
    return BOOL;
  if (type == "DATE")
    return DATE;
  if (type == "DATETIME")
    return DATETIME;
  if (type == "TIMESTAMP")
    return TIMESTAMP;
  if (type == "BIT")
    return BIT;
  if (type == "BLOB")
    return BLOB;
  if (type == "CHAR")
    return CHAR;
  if (type == "VARCHAR")
    return VARCHAR;
  if (type == "TEXT")
    return TEXT;
  if (type == "GENERATED")
    return GENERATED;
  if (type == "JSON")
    return JSON;
  return COLUMN_MAX;
}
