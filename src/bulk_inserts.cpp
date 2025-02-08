#include "random_test.hpp"
#include <document.h>
extern std::mutex all_table_mutex;
extern std::vector<Table *> *all_tables;
extern int number_of_records;
extern std::atomic<bool> run_query_failed;
using namespace rapidjson;

static int table_initial_record(std::string name) {
  std::lock_guard<std::mutex> lock(all_table_mutex);
  for (auto &table : *all_tables) {
    if (table->name_.compare(name) == 0)
      return table->number_of_initial_records;
  }
  assert(false);
  return 0;
}

bool Table::InsertBulkRecord(Thd1 *thd) {
  bool is_list_partition = false;

  // if parent has no records, child can't have records
  if (type == FK) {
    std::string parent = name_.substr(0, name_.length() - 3);
    if (table_initial_record(parent) == 0)
      number_of_initial_records = 0;
  }

  if (number_of_initial_records == 0)
    return true;

  std::string prepare_sql = "INSERT IGNORE ";

  std::vector<int> fk_unique_keys;

  /* If a table has FK move its parent keys in fk_unique_keys */
  if (type == TABLE_TYPES::FK) {
    fk_unique_keys = std::move(thd->unique_keys);
  }
  if (has_int_pk()) {
    thd->unique_keys = generateUniqueRandomNumbers(number_of_initial_records);
  }
  auto column_has_unique_key = [this](Column *col) {
    for (auto &index : *indexes_) {
      if (index->unique) {
        for (auto &ind_col : *index->columns_) {
          auto column = ind_col->column;
          if (column->type_ == Column::INT && column->name_ == col->name_)
            return true;
        }
      }
    }
    return false;
  };

  std::map<std::string, std::vector<int>> unique_keys;

  auto generate_random_fk_keys_with_unique_column = [thd, this]() {
    /* generate unique keys for FK column which picks unique value from parent
     * table */
    std::unordered_set<int> unique_keys_set(number_of_initial_records);

    if (thd->unique_keys.size() ==
        static_cast<size_t>(number_of_initial_records))
      return thd->unique_keys;

    /* populate unique_keys_set with unique keys */
    while (unique_keys_set.size() <
           static_cast<size_t>(number_of_initial_records)) {
      unique_keys_set.insert(
          thd->unique_keys.at(rand_int(number_of_initial_records)));
    }
    std::vector<int> unique_keys(unique_keys_set.begin(),
                                 unique_keys_set.end());
    return unique_keys;
  };

  for (const auto &column : *columns_) {
    if (column->primary_key)
      continue;
    if (column_has_unique_key(column)) {
      if (column->name_ == "fk_col") {
        number_of_initial_records =
            thd->unique_keys.size() <
                    static_cast<size_t>(number_of_initial_records)
                ? thd->unique_keys.size()
                : number_of_initial_records;
        unique_keys[column->name_] =
            generate_random_fk_keys_with_unique_column();
      } else {
        unique_keys[column->name_] =
            generateUniqueRandomNumbers(number_of_initial_records);
      }
    }
  }

  /* ignore error in the case parition list  */
  if (type == PARTITION &&
      static_cast<Partition *>(this)->part_type == Partition::LIST) {
    is_list_partition = true;
  }

  prepare_sql += "INTO " + name_ + " (";

  assert(number_of_initial_records <=
         (options->at(Option::UNIQUE_RANGE)->getInt() * number_of_records));

  for (const auto &column : *columns_) {
    prepare_sql += column->name_ + ", ";
  }

  prepare_sql.erase(prepare_sql.length() - 2);
  prepare_sql += ")";

  std::string values = " VALUES";
  unsigned int records = 0;

  std::vector<int> pk_insert;

  while (records < number_of_initial_records) {
    std::string value = "(";
    for (const auto &column : *columns_) {
      /* if column is part of unique index, we use the unique key */
      if (unique_keys.find(column->name_) != unique_keys.end()) {
        value += std::to_string(unique_keys.at(column->name_).at(records));
      } else if (column->name_.find("fk_col") != std::string::npos) {
        /* For FK we get the unique value from the parent table unique vector */
        value +=
            std::to_string(fk_unique_keys[rand_int(fk_unique_keys.size() - 1)]);
      } else if (column->type_ == Column::COLUMN_TYPES::GENERATED) {
        value += "DEFAULT";
      } else if (column->primary_key && column->type_ == Column::INT) {
        value += std::to_string(thd->unique_keys.at(records));
        if (options->at(Option::LOG_PK_BULK_INSERT)->getBool())
          pk_insert.push_back(thd->unique_keys.at(records));

      } else if (column->auto_increment == true) {
        value += "NULL";
      } else if (is_list_partition && column->name_.compare("ip_col") == 0) {
        /* for list partition we insert only maximum possible value
         * todo modify rand_value to return list parititon range */
        value += std::to_string(
            rand_int(maximum_records_in_each_parititon_list *
                     options->at(Option::MAX_PARTITIONS)->getInt()));
      } else {
        value += column->rand_value();
      }

      value += ", ";
    }
    value.erase(value.size() - 2);
    value += ")";
    values += value;
    records++;
    if (values.size() >
            (size_t)options->at(Option::BULK_INSERT_WIDTH)->getInt() ||
        number_of_initial_records == records) {
      if (!execute_sql(prepare_sql + values, thd)) {
        print_and_log("Bulk insert failed for table  " + name_, thd, true);
        run_query_failed = true;
        return false;
      }
      values = " VALUES";
      if (pk_insert.size() > 0) {
        thd->thread_log << "Primary key inserted : " << name_ << " ";
        for (auto &pk : pk_insert) {
          thd->thread_log << pk << " ";
        }
        thd->thread_log << std::endl;
        pk_insert.clear();
      }
    } else {
      values += ", ";
    }
  }

  return true;
}
