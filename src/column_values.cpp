#include "random_test.hpp"
#include <document.h>
extern std::mutex all_table_mutex;
extern std::vector<Table *> *all_tables;
extern int number_of_records;
extern std::atomic<bool> run_query_failed;
using namespace rapidjson;

std::string Table::ColumnValues(int value_count) {
  std::string cols = "(";
  for (auto &column : *columns_) {
    cols += column->name_ + ", ";
  }
  cols.pop_back();
  cols.pop_back();
  cols += ")";

  std::string vals;
  for (int i = 0; i < value_count; i++) {
    vals += "(";
    for (auto &column : *columns_) {
      if (column->type_ == Column::COLUMN_TYPES::GENERATED)
        vals += "DEFAULT, ";
      else if (column->auto_increment == true && rand_int(100) < 10)
        vals += "NULL, ";
      else
        vals += column->rand_value() + ", ";
    }
    vals.pop_back();
    vals.pop_back();
    vals += "), ";
  }
  vals.pop_back();
  vals.pop_back();
  return cols + " VALUES" + vals;
}
