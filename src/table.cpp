#include "random_test.hpp"
#include <array>
#include <document.h>
#ifndef __APPLE__
#include <malloc.h>
#endif
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
template <typename Container>
static void log_pk_insert(Thd1 *thd, const std::string &name_,
                          const Container &pk_insert) {
  if (pk_insert.empty()) {
    return;
  }

  thd->thread_log << "Primary key inserted: " << name_ << ' ';
  for (const auto &pk : pk_insert) {
    thd->thread_log << pk << ' ';
  }
  thd->thread_log << '\n';
}

bool Table::InsertBulkRecord(Thd1 *thd) {
  assert(number_of_initial_records <=
         (options->at(Option::UNIQUE_RANGE)->getFloat() * number_of_records));

  if (number_of_initial_records == 0)
    return true;

  long int parent_initial_record = 0;
  std::vector<long int> fk_parent_unique_keys;
  /* If a table has fk move its parent keys in fk_parent_unique_keys */
  if (type == TABLE_TYPES::FK) {
    std::string parent = name_.substr(0, name_.length() - 3);
    parent_initial_record = table_initial_record(parent);
    if (parent_initial_record == 0) {
      number_of_initial_records = 0;
      return true;
    }
    /* parent table must have been populated with autoinc values */
    if (thd->unique_keys.size() == 0) {
      fk_parent_unique_keys.resize(parent_initial_record);
      std::iota(fk_parent_unique_keys.begin(), fk_parent_unique_keys.end(), 1);
    } else {
      fk_parent_unique_keys = std::move(thd->unique_keys);
    }
  }

  thd->unique_keys.clear();

  if (has_pk()) {
    bool is_auto_increment = false;
    if (has_int_pk()) {
      for (const auto &col : *columns_) {
        if (col->primary_key && col->auto_increment) {
          is_auto_increment = true;
          break;
        }
      }
    }
    if (!is_auto_increment) {
      thd->unique_keys = generateUniqueRandomNumbers(number_of_initial_records);

      print_and_log("Generated " + toHumanReadable(thd->unique_keys.size()) +
                        " unique number for " + name_,
                    thd, false, false);

      // sort key based on type
      if (has_int_pk())
        std::sort(thd->unique_keys.begin(), thd->unique_keys.end());
      else
        std::sort(thd->unique_keys.begin(), thd->unique_keys.end(),
                  [](const long int &a, const long int &b) {
                    return std::to_string(a) < std::to_string(b);
                  });
    }
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

  std::map<std::string, std::vector<long int>> unique_keys;

  auto generate_random_fk_keys_with_unique_column = [fk_parent_unique_keys,
                                                     this]() {
    /* generate unique keys for FK column which picks unique value from parent
     * table */
    std::unordered_set<long int> unique_keys_set(number_of_initial_records);

    if (fk_parent_unique_keys.size() ==
        static_cast<size_t>(number_of_initial_records))
      return fk_parent_unique_keys;

    /* populate unique_keys_set with unique keys */
    while (unique_keys_set.size() <
           static_cast<size_t>(number_of_initial_records)) {
      unique_keys_set.insert(
          fk_parent_unique_keys.at(rand_int(fk_parent_unique_keys.size() - 1)));
    }
    std::vector<long int> unique_keys(unique_keys_set.begin(),
                                      unique_keys_set.end());
    return unique_keys;
  };

  for (const auto &column : *columns_) {
    if (column->primary_key)
      continue;
    if (column_has_unique_key(column)) {
      if (column->name_ == "fk_col") {

        number_of_initial_records =
            fk_parent_unique_keys.size() <
                    static_cast<size_t>(number_of_initial_records)
                ? fk_parent_unique_keys.size()
                : number_of_initial_records;
        unique_keys[column->name_] =
            generate_random_fk_keys_with_unique_column();
      } else {
        unique_keys[column->name_] =
            generateUniqueRandomNumbers(number_of_initial_records);
      }
    }
  }
  // to reduce space of map using in generateUniqueRandomNumbers
  malloc_trim(0);

  std::string prepare_sql = "INSERT ";
  prepare_sql += "INTO " + name_ + " (";


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
          value += std::to_string(fk_parent_unique_keys[rand_int(
              fk_parent_unique_keys.size() - 1)]);
      } else if (column->type_ == Column::COLUMN_TYPES::GENERATED) {
        value += "DEFAULT";
      } else if (column->primary_key and thd->unique_keys.size() > 0) {
        value += std::to_string(thd->unique_keys.at(records));
        if (options->at(Option::LOG_PK_BULK_INSERT)->getBool())
          pk_insert.push_back(thd->unique_keys.at(records));

      } else if (column->auto_increment == true) {
        value += "NULL";
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
      if (!execute_sql(prepare_sql + values, thd, false)) {
        print_and_log("Bulk insert failed for table  " + name_, thd, true);
        run_query_failed = true;
        return false;
      }
      values = " VALUES";
      log_pk_insert(thd, name_, pk_insert);
    } else {
      values += ", ";
    }
    // after insert million of records we write to thread log that million
    // records are insert
    if (records % 1000000 == 0) {
      print_and_log("Inserted " + toHumanReadable(records) +
                        " records in table " + name_,
                    thd, false, false);
    }
  }

  return true;
}

std::string Table::ColumnValues(Thd1 *thd, int value_count) {
  std::string cols = "(";
  for (auto &column : *columns_) {
    cols += column->name_ + ", ";
  }
  cols.pop_back();
  cols.pop_back();
  cols += ")";

  std::vector<std::string> pk_insert;
  if (value_count != 1)
    pk_insert.reserve(value_count);
  std::string vals;
  for (int i = 0; i < value_count; i++) {
    vals += "(";
    for (auto &column : *columns_) {
      if (column->type_ == Column::COLUMN_TYPES::GENERATED)
        vals += "DEFAULT, ";
      else if (column->auto_increment)
        vals += "NULL, ";
      else {
        auto rand_val = column->rand_value();
        vals += rand_val + ", ";
        if (value_count != 1 &&
            options->at(Option::LOG_PK_BULK_INSERT)->getBool() &&
            column->primary_key)
          pk_insert.push_back(rand_val);
      }
    }

    vals.pop_back();
    vals.pop_back();
    vals += "), ";
  }
  log_pk_insert(thd, name_, pk_insert);
  vals.pop_back();
  vals.pop_back();
  return cols + " VALUES" + vals;
}

void Table::InsertRandomRow(Thd1 *thd) {
  lock_table_mutex(thd->ddl_query);
  std::string sql =
      "INSERT " + add_ignore_clause() + " INTO " + name_ + ColumnValues(thd);
  unlock_table_mutex();

  std::shared_lock lock(dml_mutex);
  execute_sql(sql, thd);
}
void Table::InsertRandomRowBulk(Thd1 *thd) {
  lock_table_mutex(thd->ddl_query);

  std::string sql =
      "INSERT " + add_ignore_clause() + " INTO " + name_ +
      ColumnValues(thd, options->at(Option::INSERT_BULK_COUNT)->getInt());
  unlock_table_mutex();

  /* Hold shared dml_mutex during execute so DROP COLUMN (exclusive dml_mutex)
     cannot remove a column between SQL build and execution. */
  std::shared_lock<std::shared_mutex> lock(dml_mutex);
  execute_sql(sql, thd, false);
}

template <typename Writer> void Table::Serialize(Writer &writer) const {
  writer.StartObject();

  writer.String("name");
  writer.String(name_.c_str(), static_cast<SizeType>(name_.length()));
  writer.String("type");
  writer.String(get_type().c_str(), static_cast<SizeType>(get_type().length()));

  if (type == PARTITION) {
    auto part_table = static_cast<const Partition *>(this);
    writer.String("part_type");
    std::string part_type = part_table->get_part_type();
    writer.String(part_type.c_str(), static_cast<SizeType>(part_type.length()));
    writer.String("number_of_part");
    writer.Int(part_table->number_of_part);
    if (part_table->part_type == Partition::RANGE) {
      writer.String("part_range");
      writer.StartArray();
      for (auto par : part_table->positions) {
        writer.StartArray();
        writer.String(par.name.c_str(),
                      static_cast<SizeType>(par.name.length()));
        writer.Int(par.range);
        writer.EndArray();
      }
      writer.EndArray();
    } else if (part_table->part_type == Partition::LIST) {

      writer.String("part_list");
      writer.StartArray();
      for (auto list : part_table->lists) {
        writer.StartArray();
        writer.String(list.name.c_str(),
                      static_cast<SizeType>(list.name.length()));
        writer.StartArray();
        for (auto i : list.list)
          writer.Int(i);
        writer.EndArray();
        writer.EndArray();
      };
      writer.EndArray();
    }
  } else if (type == FK) {
    auto fk_table = static_cast<const FK_table *>(this);
    std::string on_update = fk_table->enumToString(fk_table->on_update);
    std::string on_delete = fk_table->enumToString(fk_table->on_delete);
    writer.String("on_update");
    writer.String(on_update.c_str(), static_cast<SizeType>(on_update.length()));
    writer.String("on_delete");
    writer.String(on_delete.c_str(), static_cast<SizeType>(on_delete.length()));
  }

  writer.String("engine");
  if (!engine.empty())
    writer.String(engine.c_str(), static_cast<SizeType>(engine.length()));
  else
    writer.String("default");

  writer.String("row_format");
  if (!row_format.empty())
    writer.String(row_format.c_str(),
                  static_cast<SizeType>(row_format.length()));
  else
    writer.String("default");

  writer.String("tablespace");
  if (!tablespace.empty())
    writer.String(tablespace.c_str(),
                  static_cast<SizeType>(tablespace.length()));
  else
    writer.String("file_per_table");

  writer.String("encryption");
  writer.String(encryption.c_str(), static_cast<SizeType>(encryption.length()));

  writer.String("compression");
  writer.String(compression.c_str(),
                static_cast<SizeType>(compression.length()));

  writer.String("key_block_size");
  writer.Int(key_block_size);

  writer.String("number_of_initial_records");
  writer.Int(number_of_initial_records);

  writer.String(("columns"));
  writer.StartArray();

  /* write all colummns */
  for (auto &col : *columns_) {
    writer.StartObject();
    col->Serialize(writer);
    if (col->type_ == Column::GENERATED) {
      static_cast<Generated_Column *>(col)->Serialize(writer);
    } else if (col->type_ == Column::BLOB || col->type_ == Column::TEXT) {
      static_cast<Blob_Column *>(col)->Serialize(writer);
    } else if (col->type_ == Column::ENUM) {
      static_cast<Enum_Column *>(col)->Serialize(writer);
    } else if (col->type_ == Column::DECIMAL) {
      static_cast<Decimal_Column *>(col)->Serialize(writer);
    }

    writer.EndObject();
  }

  writer.EndArray();

  writer.String(("indexes"));
  writer.StartArray();
  for (auto *ind : *indexes_)
    ind->Serialize(writer);
  writer.EndArray();
  writer.EndObject();
}
bool Table::load(Thd1 *thd, bool bulk_insert,
                 bool set_global_run_query_failed) {
  thd->ddl_query = true;
#ifdef USE_CLICKHOUSE
  /* For step=1 / prepare, drop existing table so we start from a clean schema.
     With ReplicatedMergeTree, DROP syncs to all replicas automatically. */
  if (options->at(Option::STEP)->getInt() == 1 ||
      options->at(Option::PREPARE)->getBool()) {
    execute_sql("DROP TABLE IF EXISTS " + name_, thd, false);
  }
#endif
  if (!execute_sql(definition(false), thd)) {
    if (set_global_run_query_failed) {
      print_and_log("Failed to create table " + name_, thd, true);
      run_query_failed = true;
    }
    return false;
  }

  /* load default data in table */
  if (!options->at(Option::JUST_LOAD_DDL)->getBool() && bulk_insert) {

    if (options->at(Option::WAIT_FOR_SYNC)->getBool() &&
        !options->at(Option::SECONDARY_AFTER_CREATE)->getBool())
      wait_till_sync(name_, thd);

    /* load default data in table */
    thd->ddl_query = false;
    if (!InsertBulkRecord(thd))
      return false;
  }

  if (options->at(Option::SECONDARY_AFTER_CREATE)->getBool()) {
    if (!execute_sql("ALTER TABLE " + name_ + " SECONDARY_ENGINE=" +
                         options->at(Option::SECONDARY_ENGINE)->getString(),
                     thd)) {
      print_and_log("Failed to set secondary engine for table " + name_, thd,
                    true);
      return false;
    }
    if (options->at(Option::WAIT_FOR_SYNC)->getBool()) {
      wait_till_sync(name_, thd);
    }
  }

  thd->ddl_query = true;
  if (!load_secondary_indexes(thd)) {
    return false;
  }

  if (this->type == Table::TABLE_TYPES::FK) {
    if (!static_cast<FK_table *>(this)->load_fk_constrain(
            thd, set_global_run_query_failed)) {
      return false;
    }
  }

  return true;
}

template <typename Writer> void Column::Serialize(Writer &writer) const {
  writer.String("name");
  writer.String(name_.c_str(), static_cast<SizeType>(name_.length()));
  writer.String("type");
  std::string typ = col_type_to_string(type_);
  writer.String(typ.c_str(), static_cast<SizeType>(typ.length()));
  writer.String("null_val");
  writer.Bool(null_val);
  writer.String("primary_key");
  writer.Bool(primary_key);
  writer.String("compressed");
  writer.Bool(compressed);
  writer.String("auto_increment");
  writer.Bool(auto_increment);
  writer.String("not secondary");
  writer.Bool(not_secondary);
  writer.String("is_partition");
  writer.Bool(is_partition);
  writer.String("length");
  writer.Int(length);
}
/* add sub_type metadata */
template <typename Writer> void Blob_Column::Serialize(Writer &writer) const {
  writer.String("sub_type");
  writer.String(sub_type.c_str(), static_cast<SizeType>(sub_type.length()));
}
template <typename Writer>
void Decimal_Column::Serialize(Writer &writer) const {
  writer.String("precision");
  writer.Int(precision);
}

template <typename Writer> void Enum_Column::Serialize(Writer &writer) const {
  /* write all enum values */
  writer.String("enum_values");
  writer.StartArray();
  for (auto &val : enum_values) {
    writer.String(val.c_str(), static_cast<SizeType>(val.length()));
  }
  writer.EndArray();
}

/* add sub_type and clause in metadata */
template <typename Writer>
void Generated_Column::Serialize(Writer &writer) const {
  writer.String("sub_type");
  auto type = col_type_to_string(g_type);
  writer.String(type.c_str(), static_cast<SizeType>(type.length()));
  writer.String("clause");
  writer.String(str.c_str(), static_cast<SizeType>(str.length()));
}
template <typename Writer> void Ind_col::Serialize(Writer &writer) const {
  writer.StartObject();
  writer.String("name");
  auto &name = column->name_;
  writer.String(name.c_str(), static_cast<SizeType>(name.length()));
  writer.String("desc");
  writer.Bool(desc);
  writer.String("length");
  writer.Uint(length);
  writer.EndObject();
}

template <typename Writer> void Index::Serialize(Writer &writer) const {
  writer.StartObject();
  writer.String("name");
  writer.String(name_.c_str(), static_cast<SizeType>(name_.length()));
  writer.String("unique");
  writer.Bool(unique);
  writer.String(("index_columns"));
  writer.StartArray();
  for (auto ic : *columns_)
    ic->Serialize(writer);
  writer.EndArray();
  writer.EndObject();
}
/* step file name based on the database and step */
std::string build_step_file_name(int step) {
  std::string path = opt_string(METADATA_PATH);
  if (path.size() == 0 || step != 0)
    path = opt_string(LOGDIR);
#ifdef USE_MYSQL
  const std::string instance = "mysql";
#elif USE_DUCKDB
  const std::string instance = "duckdb";
#elif USE_CLICKHOUSE
  const std::string instance = "clickhouse";
#endif
  auto file = path + "/" + instance + "_" +
              options->at(Option::DATABASE)->getString() + "_metadata_step_" +
              std::to_string(step) + ".log";
  return file;
}

/* save metadata to a file */
void save_metadata_to_file() {
  auto file = build_step_file_name(options->at(Option::STEP)->getInt());
  std::cout << "Saving metadata to file " << file << std::endl;

  StringBuffer sb;
  PrettyWriter<StringBuffer> writer(sb);
  writer.StartObject();
  writer.String("version");
  writer.Uint(version);
  writer.String(("tables"));
  writer.StartArray();
  for (auto j = all_tables->begin(); j != all_tables->end(); j++) {
    auto table = *j;
    table->Serialize(writer);
  }
  writer.EndArray();
  writer.EndObject();
  std::ofstream of(file);
  of << sb.GetString();

  if (!of.good()) {
    print_and_log("can't write the JSON string to the file!", nullptr);
    exit(EXIT_FAILURE);
  }
}
/*load objects from a file */
std::string load_metadata_from_file() {
  auto previous_step = options->at(Option::STEP)->getInt() - 1;
  auto file = build_step_file_name(previous_step);
  FILE *fp = fopen(file.c_str(), "r");

  if (fp == nullptr) {
    print_and_log("Unable to find metadata file " + file, nullptr);
    return "FAILED";
  }
  char readBuffer[65536];
  FileReadStream is(fp, readBuffer, sizeof(readBuffer));
  Document d;
  d.ParseStream(is);
  auto v = d["version"].GetInt();

  if (d["version"].GetInt() != version) {
    print_and_log("version mismatch between " + file + " and codebase " +
                      " file::version is " + std::to_string(v) +
                      " code::version is " + std::to_string(version),
                  nullptr);
    exit(EXIT_FAILURE);
  }

  for (auto &tab : d["tables"].GetArray()) {
    Table *table;
    std::string name = tab["name"].GetString();
    std::string table_type = tab["type"].GetString();

    if (table_type.compare("PARTITION") == 0) {
      std::string part_type = tab["part_type"].GetString();
      table = new Partition(name, part_type, tab["number_of_part"].GetInt());

      if (part_type.compare("RANGE") == 0) {
        for (auto &par_range : tab["part_range"].GetArray()) {
          static_cast<Partition *>(table)->positions.emplace_back(
              par_range[0].GetString(), par_range[1].GetInt());
        }
      } else if (part_type.compare("LIST") == 0) {
        int curr_index_of_list = 0;
        for (auto &par_list : tab["part_list"].GetArray()) {
          static_cast<Partition *>(table)->lists.emplace_back(
              par_list[0].GetString());
          for (auto &list_value : par_list[1].GetArray())
            static_cast<Partition *>(table)
                ->lists.at(curr_index_of_list)
                .list.push_back(list_value.GetInt());
          curr_index_of_list++;
        }
      }
    } else if (table_type.compare("NORMAL") == 0) {
      table = new Table(name);
    } else if (table_type == "FK") {
      std::string on_update = tab["on_update"].GetString();
      std::string on_delete = tab["on_delete"].GetString();
      table = new FK_table(name, on_update, on_delete);
    } else {
      print_and_log("Unhandle Table type " + table_type, nullptr);
      exit(EXIT_FAILURE);
    }

    table->set_type(table_type);

    std::string engine = tab["engine"].GetString();
    if (engine.compare("default") != 0) {
      table->engine = engine;
    }

    std::string row_format = tab["row_format"].GetString();
    if (row_format.compare("default") != 0) {
      table->row_format = row_format;
    }

    std::string tablespace = tab["tablespace"].GetString();
    if (tablespace.compare("file_per_table") != 0) {
      table->tablespace = tablespace;
    }

    table->encryption = tab["encryption"].GetString();
    table->compression = tab["compression"].GetString();

    table->key_block_size = tab["key_block_size"].GetInt();
    table->number_of_initial_records =
        tab["number_of_initial_records"].GetInt();

    /* save columns */
    for (auto &col : tab["columns"].GetArray()) {
      Column *a;
      std::string type = col["type"].GetString();

      const std::array<std::string, 12> column_types{
          {"INT", "CHAR", "VARCHAR", "BOOL", "FLOAT", "DOUBLE", "INTEGER",
           "DATE", "DATETIME", "TIMESTAMP", "BIT", "JSON"}};
      auto isValidType =
          std::find(column_types.begin(), column_types.end(), type);

      if (isValidType != column_types.end()) {
        a = new Column(col["name"].GetString(), type, table);
      } else if (type.compare("GENERATED") == 0) {
        auto name = col["name"].GetString();
        auto clause = col["clause"].GetString();
        auto sub_type = col["sub_type"].GetString();
        a = new Generated_Column(name, table, clause, sub_type);
      } else if (type.compare("BLOB") == 0) {
        auto sub_type = col["sub_type"].GetString();
        a = new Blob_Column(col["name"].GetString(), table, sub_type);
      } else if (type.compare("TEXT") == 0) {
        auto sub_type = col["sub_type"].GetString();
        a = new Text_Column(col["name"].GetString(), table, sub_type);
      } else if (type.compare("DECIMAL") == 0) {
        a = new Decimal_Column(col["name"].GetString(), table,
                               col["precision"].GetInt(),
                               col["length"].GetInt());
      } else if (type.compare("ENUM") == 0) {
        auto enum_values = col["enum_values"].GetArray();
        std::vector<std::string> enum_values_;
        for (auto &val : enum_values) {
          enum_values_.push_back(val.GetString());
        }
        a = new Enum_Column(col["name"].GetString(), table,
                            std::move(enum_values_));
      } else {
        print_and_log("unhandled column type " + type, nullptr);
        exit(EXIT_FAILURE);
      }

      a->null_val = col["null_val"].GetBool();
      a->is_partition = col["is_partition"].GetBool();
      a->auto_increment = col["auto_increment"].GetBool();
      a->length = col["length"].GetInt(),
      a->primary_key = col["primary_key"].GetBool();
      a->compressed = col["compressed"].GetBool();
      a->not_secondary = col["not secondary"].GetBool();
      table->AddInternalColumn(a);
    }

    for (auto &ind : tab["indexes"].GetArray()) {
      Index *index =
          new Index(ind["name"].GetString(), ind["unique"].GetBool());

      for (auto &ind_col : ind["index_columns"].GetArray()) {
        std::string index_base_column = ind_col["name"].GetString();

        for (auto &column : *table->columns_) {
          if (index_base_column.compare(column->name_) == 0) {
            index->AddInternalColumn(
                new Ind_col(column, ind_col["desc"].GetBool()));
            break;
          }
        }
      }
      table->AddInternalIndex(index);
    }

    all_tables->push_back(table);
  }

  fclose(fp);
  print_and_log("metadata loaded from file " + file, nullptr);
  return file;
}
